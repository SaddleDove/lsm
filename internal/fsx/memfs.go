package fsx

import (
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type inode struct {
	mu         sync.Mutex
	data       []byte
	durableLen int
	deleted    bool
	mode       os.FileMode
	mod        time.Time
	dir        bool
}

type renameRec struct {
	from, to string
	durable  bool
}

type MemFS struct {
	mu           sync.Mutex
	files        map[string]*inode
	stats        Stats
	policy       *CrashPolicy
	pending      []renameRec
	seq          int64
	lastSyncPath string
}

func NewMemFS() *MemFS {
	m := &MemFS{files: make(map[string]*inode)}
	m.files["/"] = &inode{dir: true, mode: 0755 | os.ModeDir, mod: time.Now()}
	return m
}

func (m *MemFS) SetPolicy(p *CrashPolicy) { m.policy = p }

func (m *MemFS) Policy() *CrashPolicy { return m.policy }

func (m *MemFS) Stats() *Stats { return &m.stats }

func (m *MemFS) norm(name string) string {
	name = path.Clean("/" + strings.TrimPrefix(name, "/"))
	if name == "." {
		return "/"
	}
	return name
}

func (m *MemFS) parent(name string) string {
	d := path.Dir(name)
	if d == "" {
		return "/"
	}
	return d
}

func (m *MemFS) ensureDir(name string) error {
	name = m.norm(name)
	if name == "/" {
		return nil
	}
	if err := m.ensureDir(m.parent(name)); err != nil {
		return err
	}
	if in, ok := m.files[name]; ok {
		if !in.dir {
			return os.ErrExist
		}
		return nil
	}
	m.files[name] = &inode{dir: true, mode: 0755 | os.ModeDir, mod: time.Now()}
	return nil
}

func (m *MemFS) MkdirAll(name string, perm os.FileMode) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ensureDir(m.norm(name))
}

func (m *MemFS) Create(name string) (File, error) {
	return m.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
}

func (m *MemFS) Open(name string) (File, error) {
	return m.OpenFile(name, os.O_RDONLY, 0)
}

func (m *MemFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	name = m.norm(name)
	if err := m.policy.hit(Op{Kind: OpCreate, Path: name}); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ensureDir(m.parent(name)); err != nil {
		return nil, err
	}
	in, ok := m.files[name]
	if !ok {
		if flag&os.O_CREATE == 0 {
			return nil, os.ErrNotExist
		}
		in = &inode{mode: perm, mod: time.Now()}
		m.files[name] = in
		m.stats.FilesCreated.Add(1)
	} else if in.dir {
		return nil, os.ErrInvalid
	} else if in.deleted {
		if flag&os.O_CREATE == 0 {
			return nil, os.ErrNotExist
		}
		in.deleted = false
		in.data = nil
		in.durableLen = 0
		m.stats.FilesCreated.Add(1)
	} else if flag&os.O_TRUNC != 0 {
		in.data = nil
		in.durableLen = 0
	}
	m.stats.OpCount.Add(1)
	return &memFile{fs: m, name: name, in: in, off: 0, flag: flag}, nil
}

func (m *MemFS) Remove(name string) error {
	name = m.norm(name)
	if err := m.policy.hit(Op{Kind: OpRemove, Path: name}); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.files[name]
	if !ok || in.deleted {
		return os.ErrNotExist
	}
	in.deleted = true
	in.data = nil
	in.durableLen = 0
	delete(m.files, name)
	m.stats.FilesRemoved.Add(1)
	m.stats.OpCount.Add(1)
	return nil
}

func (m *MemFS) Rename(old, new string) error {
	old, new = m.norm(old), m.norm(new)
	if err := m.policy.hit(Op{Kind: OpRename, Path: old, Dst: new}); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.files[old]
	if !ok || in.deleted {
		return os.ErrNotExist
	}
	if dst, ok := m.files[new]; ok && !dst.deleted {
		dst.deleted = true
	}
	m.files[new] = in
	delete(m.files, old)
	m.pending = append(m.pending, renameRec{from: old, to: new, durable: false})
	m.stats.Renames.Add(1)
	m.stats.OpCount.Add(1)
	return nil
}

func (m *MemFS) ReadDir(name string) ([]os.DirEntry, error) {
	name = m.norm(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.files[name]
	if !ok || in.deleted {
		return nil, os.ErrNotExist
	}
	prefix := name
	if prefix != "/" {
		prefix += "/"
	}
	var out []os.DirEntry
	seen := map[string]bool{}
	for p, n := range m.files {
		if n.deleted || p == name {
			continue
		}
		if name == "/" {
			rest := strings.TrimPrefix(p, "/")
			if rest == p && p != "/" {
				continue
			}
			first := rest
			if i := strings.IndexByte(rest, '/'); i >= 0 {
				first = rest[:i]
			}
			if first == "" || seen[first] {
				continue
			}
			seen[first] = true
			child := m.files["/"+first]
			if child == nil {
				continue
			}
			out = append(out, dirEnt{name: first, mode: child.mode, size: int64(len(child.data)), mod: child.mod, dir: child.dir})
			continue
		}
		if !strings.HasPrefix(p, prefix) {
			continue
		}
		rest := p[len(prefix):]
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			rest = rest[:i]
		}
		if rest == "" || seen[rest] {
			continue
		}
		seen[rest] = true
		child := m.files[prefix+rest]
		if child == nil {
			continue
		}
		out = append(out, dirEnt{name: rest, mode: child.mode, size: int64(len(child.data)), mod: child.mod, dir: child.dir})
	}
	return out, nil
}

func (m *MemFS) Stat(name string) (os.FileInfo, error) {
	name = m.norm(name)
	m.mu.Lock()
	defer m.mu.Unlock()
	in, ok := m.files[name]
	if !ok || in.deleted {
		return nil, os.ErrNotExist
	}
	return fileInfo{name: path.Base(name), size: int64(len(in.data)), mode: in.mode, mod: in.mod, dir: in.dir}, nil
}

func (m *MemFS) SyncDir(name string) error {
	name = m.norm(name)
	if err := m.policy.hit(Op{Kind: OpDirFsync, Path: name}); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.pending {
		m.pending[i].durable = true
	}
	m.stats.DirFsyncs.Add(1)
	m.stats.OpCount.Add(1)
	return nil
}

func (m *MemFS) Crash() *MemFS {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := NewMemFS()
	next.policy = nil
	tear := 0
	tearPath := m.lastSyncPath
	if m.policy != nil && m.policy.TearPath != "" {
		// Explicit target: tear a named file's durable tail (e.g. CURRENT).
		tearPath = m.norm(m.policy.TearPath)
		tear = m.policy.TearBytes
		if tear <= 0 {
			tear = 7
		}
	} else if m.policy != nil && m.policy.TearLast {
		tear = m.policy.TearBytes
		if tear <= 0 {
			tear = 7
		}
	}
	undone := map[string]string{}
	for i := len(m.pending) - 1; i >= 0; i-- {
		r := m.pending[i]
		if !r.durable {
			undone[r.to] = r.from
		}
	}
	for p, in := range m.files {
		if in.dir {
			_ = next.ensureDir(p)
			continue
		}
		if in.deleted {
			continue
		}
		final := p
		if orig, ok := undone[p]; ok {
			final = orig
		}
		n := in.durableLen
		if n < 0 {
			n = 0
		}
		if n > len(in.data) {
			n = len(in.data)
		}
		buf := make([]byte, n)
		copy(buf, in.data[:n])
		if tear > 0 && n > 0 && p == tearPath {
			t := tear
			if t > n {
				t = n
			}
			buf = buf[:n-t]
		}
		ni := &inode{data: buf, durableLen: len(buf), mode: in.mode, mod: in.mod}
		next.files[final] = ni
	}
	return next
}

func (m *MemFS) Clone() *MemFS {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := NewMemFS()
	for p, in := range m.files {
		buf := make([]byte, len(in.data))
		copy(buf, in.data)
		next.files[p] = &inode{
			data:       buf,
			durableLen: in.durableLen,
			deleted:    in.deleted,
			mode:       in.mode,
			mod:        in.mod,
			dir:        in.dir,
		}
	}
	return next
}

type memFile struct {
	fs   *MemFS
	name string
	in   *inode
	off  int64
	flag int
}

func (f *memFile) Name() string { return f.name }

func (f *memFile) Read(p []byte) (int, error) {
	n, err := f.ReadAt(p, f.off)
	f.off += int64(n)
	return n, err
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.in.mu.Lock()
	defer f.in.mu.Unlock()
	if off >= int64(len(f.in.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.in.data[off:])
	f.fs.stats.BytesRead.Add(int64(n))
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	off := f.off
	if f.flag&os.O_APPEND != 0 {
		f.in.mu.Lock()
		off = int64(len(f.in.data))
		f.in.mu.Unlock()
	}
	n, err := f.WriteAt(p, off)
	f.off = off + int64(n)
	return n, err
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.fs.policy.hit(Op{Kind: OpWrite, Path: f.name, Off: off, Len: len(p)}); err != nil {
		return 0, err
	}
	f.in.mu.Lock()
	defer f.in.mu.Unlock()
	end := int(off) + len(p)
	if end > len(f.in.data) {
		// Grow geometrically so appending a large file is O(n), not O(n^2).
		if end <= cap(f.in.data) {
			f.in.data = f.in.data[:end]
		} else {
			ncap := cap(f.in.data) * 2
			if ncap < end {
				ncap = end
			}
			nb := make([]byte, end, ncap)
			copy(nb, f.in.data)
			f.in.data = nb
		}
	}
	copy(f.in.data[off:], p)
	f.in.mod = time.Now()
	f.fs.stats.BytesWritten.Add(int64(len(p)))
	f.fs.stats.OpCount.Add(1)
	return len(p), nil
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = f.off + offset
	case io.SeekEnd:
		f.in.mu.Lock()
		abs = int64(len(f.in.data)) + offset
		f.in.mu.Unlock()
	default:
		return 0, os.ErrInvalid
	}
	if abs < 0 {
		return 0, os.ErrInvalid
	}
	f.off = abs
	return abs, nil
}

func (f *memFile) Sync() error {
	if err := f.fs.policy.hit(Op{Kind: OpFsync, Path: f.name}); err != nil {
		return err
	}
	f.in.mu.Lock()
	n := len(f.in.data)
	f.in.durableLen = n
	f.in.mu.Unlock()
	f.fs.mu.Lock()
	f.fs.lastSyncPath = f.name
	f.fs.mu.Unlock()
	f.fs.stats.Fsyncs.Add(1)
	f.fs.stats.BytesSynced.Add(int64(n))
	f.fs.stats.OpCount.Add(1)
	return nil
}

func (f *memFile) Stat() (os.FileInfo, error) {
	f.in.mu.Lock()
	defer f.in.mu.Unlock()
	return fileInfo{name: path.Base(f.name), size: int64(len(f.in.data)), mode: f.in.mode, mod: f.in.mod}, nil
}

func (f *memFile) Truncate(size int64) error {
	if err := f.fs.policy.hit(Op{Kind: OpTruncate, Path: f.name, Off: size}); err != nil {
		return err
	}
	f.in.mu.Lock()
	defer f.in.mu.Unlock()
	if size < 0 {
		return os.ErrInvalid
	}
	if int(size) < len(f.in.data) {
		f.in.data = f.in.data[:size]
	} else {
		nb := make([]byte, size)
		copy(nb, f.in.data)
		f.in.data = nb
	}
	if f.in.durableLen > int(size) {
		f.in.durableLen = int(size)
	}
	return nil
}

func (f *memFile) Close() error { return nil }

type fileInfo struct {
	name string
	size int64
	mode os.FileMode
	mod  time.Time
	dir  bool
}

func (fi fileInfo) Name() string       { return fi.name }
func (fi fileInfo) Size() int64        { return fi.size }
func (fi fileInfo) Mode() os.FileMode  { return fi.mode }
func (fi fileInfo) ModTime() time.Time { return fi.mod }
func (fi fileInfo) IsDir() bool        { return fi.dir }
func (fi fileInfo) Sys() any           { return nil }

type dirEnt struct {
	name string
	mode os.FileMode
	size int64
	mod  time.Time
	dir  bool
}

func (d dirEnt) Name() string      { return d.name }
func (d dirEnt) IsDir() bool       { return d.dir }
func (d dirEnt) Type() os.FileMode { return d.mode }
func (d dirEnt) Info() (os.FileInfo, error) {
	return fileInfo{name: d.name, size: d.size, mode: d.mode, mod: d.mod, dir: d.dir}, nil
}

var _ fs.DirEntry = dirEnt{}
