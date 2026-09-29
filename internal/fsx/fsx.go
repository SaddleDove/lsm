package fsx

import (
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type OpKind int

const (
	OpCreate OpKind = iota
	OpWrite
	OpFsync
	OpDirFsync
	OpRename
	OpRemove
	OpTruncate
)

func (k OpKind) String() string {
	switch k {
	case OpCreate:
		return "create"
	case OpWrite:
		return "write"
	case OpFsync:
		return "fsync"
	case OpDirFsync:
		return "dirfsync"
	case OpRename:
		return "rename"
	case OpRemove:
		return "remove"
	case OpTruncate:
		return "truncate"
	default:
		return "?"
	}
}

type Op struct {
	Kind OpKind
	Path string
	Dst  string
	Off  int64
	Len  int
}

var ErrCrashed = errors.New("fsx: injected crash")

type Stats struct {
	BytesWritten atomic.Int64
	BytesSynced  atomic.Int64
	BytesRead    atomic.Int64
	FilesCreated atomic.Int64
	FilesRemoved atomic.Int64
	Fsyncs       atomic.Int64
	DirFsyncs    atomic.Int64
	Renames      atomic.Int64
	OpCount      atomic.Int64
}

func (s *Stats) Snapshot() map[string]int64 {
	return map[string]int64{
		"bytes_written": s.BytesWritten.Load(),
		"bytes_synced":  s.BytesSynced.Load(),
		"read_bytes":    s.BytesRead.Load(),
		"files_created": s.FilesCreated.Load(),
		"files_removed": s.FilesRemoved.Load(),
		"fsyncs":        s.Fsyncs.Load(),
		"dir_fsyncs":    s.DirFsyncs.Load(),
		"renames":       s.Renames.Load(),
		"ops":           s.OpCount.Load(),
	}
}

func (s *Stats) Reset() {
	s.BytesWritten.Store(0)
	s.BytesSynced.Store(0)
	s.BytesRead.Store(0)
	s.FilesCreated.Store(0)
	s.FilesRemoved.Store(0)
	s.Fsyncs.Store(0)
	s.DirFsyncs.Store(0)
	s.Renames.Store(0)
	s.OpCount.Store(0)
}

type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.WriterAt
	io.Seeker
	io.Closer
	Sync() error
	Stat() (os.FileInfo, error)
	Truncate(int64) error
	Name() string
}

type FS interface {
	Create(name string) (File, error)
	Open(name string) (File, error)
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	Remove(name string) error
	Rename(old, new string) error
	MkdirAll(name string, perm os.FileMode) error
	ReadDir(name string) ([]os.DirEntry, error)
	Stat(name string) (os.FileInfo, error)
	SyncDir(name string) error
	Stats() *Stats
}

type CrashPolicy struct {
	CrashAt   int64
	TearLast  bool
	TearPath  string
	TearBytes int
	mu        sync.Mutex
	ops       int64
	crashed   bool
}

func (p *CrashPolicy) hit(op Op) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.crashed {
		return ErrCrashed
	}
	p.ops++
	if p.CrashAt > 0 && p.ops == p.CrashAt {
		p.crashed = true
		return ErrCrashed
	}
	return nil
}

func (p *CrashPolicy) OpCount() int64 {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ops
}

func (p *CrashPolicy) Crashed() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.crashed
}
