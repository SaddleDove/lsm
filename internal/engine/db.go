package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"sync"
	"sync/atomic"

	"lsm/internal/fsx"
	"lsm/internal/ikey"
	"lsm/internal/memtable"
	"lsm/internal/sstable"
	"lsm/internal/version"
	"lsm/internal/wal"
)

var (
	ErrClosed   = errors.New("engine: closed")
	ErrNotFound = errors.New("engine: not found")
	ErrCorrupt  = errors.New("engine: silent corruption detected")
	ErrReadOnly = errors.New("engine: read only")
)

type DB struct {
	opt Options
	fs  fsx.FS
	dir string

	mu       sync.Mutex
	mem      *memtable.Skiplist
	imm      *memtable.Skiplist
	log      *wal.Writer
	vlog     *version.Log
	vs       *version.Set
	seq      uint64
	nextFile uint64
	snaps    snapList
	closed   bool
	tables   map[uint64]*sstable.Reader

	bytesIn  atomic.Int64
	sstReads atomic.Int64
}

func Open(opt Options) (*DB, error) {
	opt.fill()
	if opt.FS == nil {
		osfs, err := fsx.NewOSFS(opt.Dir)
		if err != nil {
			return nil, err
		}
		opt.FS = osfs
	}
	if err := opt.FS.MkdirAll(opt.Dir, 0755); err != nil {
		return nil, err
	}
	db := &DB{
		opt:    opt,
		fs:     opt.FS,
		dir:    opt.Dir,
		mem:    memtable.New(),
		tables: make(map[uint64]*sstable.Reader),
	}
	db.snaps.init()
	if err := db.recover(); err != nil {
		return nil, err
	}
	return db, nil
}

// bootstrap creates a fresh database (no CURRENT / no MANIFEST yet). A missing
// CURRENT with a MANIFEST already on disk is *not* fresh: that is a damaged
// directory and must fail fast (DESIGN 8.2 c8 / 317).
func (db *DB) bootstrap() error {
	vs := version.NewSet(db.opt.MaxLevel)
	vs.NextFile = 2
	vs.LogNumber = 1
	vs.Manifest = 1
	l, err := version.CreateManifest(db.fs, db.dir, 1, vs)
	if err != nil {
		return err
	}
	db.vlog = l
	db.vs = vs
	db.nextFile = vs.NextFile
	db.seq = vs.LastSeq
	w, err := wal.OpenWriter(db.fs, db.dir, vs.LogNumber, db.opt.Sync != SyncNone)
	if err != nil {
		return err
	}
	db.log = w
	return db.openTables()
}

func (db *DB) recover() error {
	l, vs, err := version.OpenManifest(db.fs, db.dir)
	if errors.Is(err, os.ErrNotExist) {
		if version.HasManifest(db.fs, db.dir) {
			return fmt.Errorf("engine: CURRENT missing but MANIFEST present: %w", version.ErrCorruptManifest)
		}
		return db.bootstrap()
	}
	if err != nil {
		return err
	}
	db.vlog = l
	db.vs = vs
	db.nextFile = vs.NextFile
	if db.nextFile == 0 {
		db.nextFile = 1
	}
	for _, f := range vs.AllFiles() {
		if f.Num+1 > db.nextFile {
			db.nextFile = f.Num + 1
		}
	}
	if vs.LogNumber+1 > db.nextFile {
		db.nextFile = vs.LogNumber + 1
	}
	if vs.Manifest+1 > db.nextFile {
		db.nextFile = vs.Manifest + 1
	}
	if ents, err := db.fs.ReadDir(db.dir); err == nil {
		for _, e := range ents {
			var n uint64
			if _, err := fmt.Sscanf(e.Name(), "%d.sst", &n); err == nil && n+1 > db.nextFile {
				db.nextFile = n + 1
			}
			if _, err := fmt.Sscanf(e.Name(), "%d.log", &n); err == nil && n+1 > db.nextFile {
				db.nextFile = n + 1
			}
			if _, err := fmt.Sscanf(e.Name(), "MANIFEST-%d", &n); err == nil && n+1 > db.nextFile {
				db.nextFile = n + 1
			}
		}
	}
	db.seq = vs.LastSeq
	if err := db.openTables(); err != nil {
		return err
	}
	minSeq := vs.LastSeq
	last, _, err := wal.Replay(db.fs, db.dir, minSeq, func(r wal.Record) error {
		ik := ikey.Encode(r.Key, r.Seq, r.Kind)
		db.mem.Put(ik, r.Val)
		return nil
	})
	if err != nil {
		return err
	}
	if last > db.seq {
		db.seq = last
	}
	logNum := vs.LogNumber
	if logNum == 0 {
		logNum = 1
	}
	w, err := wal.OpenWriter(db.fs, db.dir, logNum, db.opt.Sync != SyncNone)
	if err != nil {
		return err
	}
	db.log = w
	db.gcOrphans()
	return nil
}

func (db *DB) gcOrphans() {
	ents, err := db.fs.ReadDir(db.dir)
	if err != nil {
		return
	}
	live := db.vs.FileNums()
	liveMan := db.vs.Manifest
	liveLog := db.vs.LogNumber
	for _, e := range ents {
		name := e.Name()
		var n uint64
		if _, err := fmt.Sscanf(name, "%d.sst", &n); err == nil {
			if !live[n] {
				_ = db.fs.Remove(path.Join(db.dir, name))
			}
			continue
		}
		if _, err := fmt.Sscanf(name, "%d.log", &n); err == nil {
			// Only the manifest's current log_number is live. Everything else is
			// either an already-flushed old log or an orphan left by a crash during
			// flush; both are safe to drop (records were replayed above and
			// filtered by seq). Removing only n < liveLog used to leak newer
			// orphans indefinitely, so remove any non-live log.
			if liveLog == 0 || n != liveLog {
				_ = db.fs.Remove(path.Join(db.dir, name))
			}
			continue
		}
		if _, err := fmt.Sscanf(name, "MANIFEST-%d", &n); err == nil {
			if liveMan != 0 && n != liveMan {
				_ = db.fs.Remove(path.Join(db.dir, name))
			}
			continue
		}
		if len(name) >= 4 && name[len(name)-4:] == ".tmp" {
			_ = db.fs.Remove(path.Join(db.dir, name))
		}
	}
}

func (db *DB) openTables() error {
	for _, f := range db.vs.AllFiles() {
		if _, ok := db.tables[f.Num]; ok {
			continue
		}
		rd, err := sstable.Open(db.fs, db.dir, f.Num)
		if err != nil {
			return fmt.Errorf("open sst %d: %w", f.Num, err)
		}
		db.tables[f.Num] = rd
	}
	return nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	if db.log != nil {
		_ = db.log.Sync()
		_ = db.log.Close()
	}
	if db.vlog != nil {
		_ = db.vlog.Close()
	}
	for _, t := range db.tables {
		_ = t.Close()
	}
	return nil
}

func (db *DB) Put(k, v []byte) error {
	var b WriteBatch
	b.Put(k, v)
	return db.Write(&b, WriteOptions{Sync: db.opt.Sync == SyncEveryWrite})
}

func (db *DB) Delete(k []byte) error {
	var b WriteBatch
	b.Delete(k)
	return db.Write(&b, WriteOptions{Sync: db.opt.Sync == SyncEveryWrite})
}

func (db *DB) Write(b *WriteBatch, wo WriteOptions) error {
	if b.Count() == 0 {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opt.ReadOnly {
		return ErrReadOnly
	}
	start := db.seq + 1
	recs := make([]wal.Record, 0, len(b.Entries))
	for i, e := range b.Entries {
		seq := start + uint64(i)
		kind := ikey.KindValue
		if e.Del {
			kind = ikey.KindDelete
		}
		recs = append(recs, wal.Record{Seq: seq, Kind: kind, Key: e.Key, Val: e.Value})
	}
	if err := db.log.Append(recs); err != nil {
		return err
	}
	if wo.Sync && db.opt.Sync != SyncEveryWrite {
		if err := db.log.Sync(); err != nil {
			return err
		}
	}
	for _, rec := range recs {
		ik := ikey.Encode(rec.Key, rec.Seq, rec.Kind)
		db.mem.Put(ik, rec.Val)
		db.bytesIn.Add(int64(len(rec.Key) + len(rec.Val)))
	}
	db.seq = start + uint64(len(b.Entries)) - 1
	if db.mem.ApproximateBytes() >= db.opt.MemtableBytes {
		if err := db.flushLocked(); err != nil {
			return err
		}
		if err := db.maybeCompactLocked(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Get(k []byte) ([]byte, error) {
	db.mu.Lock()
	seq := db.seq
	db.mu.Unlock()
	return db.get(k, seq)
}

func (db *DB) get(k []byte, seq uint64) ([]byte, error) {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, ErrClosed
	}
	mem := db.mem
	imm := db.imm
	if v, ok, del := mem.Get(k, seq); ok {
		db.mu.Unlock()
		if del {
			return nil, ErrNotFound
		}
		return v, nil
	}
	if imm != nil {
		if v, ok, del := imm.Get(k, seq); ok {
			db.mu.Unlock()
			if del {
				return nil, ErrNotFound
			}
			return v, nil
		}
	}
	vs := db.vs.Clone()
	tables := make(map[uint64]*sstable.Reader, len(db.tables))
	for n, t := range db.tables {
		tables[n] = t
	}
	db.mu.Unlock()
	uk := k
	sstReads := 0
	for level, files := range vs.Levels {
		cands := files
		if level > 0 {
			cands = overlapping(files, uk, uk)
		}
		if level == 0 {
			sort.Slice(cands, func(i, j int) bool { return cands[i].Num > cands[j].Num })
		}
		for _, f := range cands {
			if !ikey.RangeMeta(f.Smallest, f.Largest, uk) && level > 0 {
				continue
			}
			rd := tables[f.Num]
			if rd == nil {
				continue
			}
			sstReads++
			v, ok, del, err := rd.Get(uk, seq)
			if err != nil {
				if errors.Is(err, sstable.ErrCorruptBlock) || errors.Is(err, sstable.ErrCorruptTable) {
					return nil, ErrCorrupt
				}
				return nil, err
			}
			if ok {
				db.sstReads.Add(int64(sstReads))
				if del {
					return nil, ErrNotFound
				}
				return v, nil
			}
		}
	}
	db.sstReads.Add(int64(sstReads))
	return nil, ErrNotFound
}

func overlapping(files []version.FileMeta, lo, hi []byte) []version.FileMeta {
	var out []version.FileMeta
	for _, f := range files {
		if bytes.Compare(ikey.UserKey(f.Largest), lo) < 0 {
			continue
		}
		if bytes.Compare(ikey.UserKey(f.Smallest), hi) > 0 {
			continue
		}
		out = append(out, f)
	}
	return out
}

func (db *DB) Snapshot() *Snapshot {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.snaps.newest(db.seq, db)
}

func (s *Snapshot) Release() {
	if s.db != nil {
		s.db.snaps.release(s)
	}
}

func (s *Snapshot) Get(k []byte) ([]byte, error) {
	return s.db.get(k, s.seq)
}

func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flushLocked()
}

func (db *DB) CompactAll() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.flushLocked(); err != nil {
		return err
	}
	for i := 0; i < 32; i++ {
		did, err := db.compactOnceLocked()
		if err != nil {
			return err
		}
		if !did {
			break
		}
	}
	return nil
}

func (db *DB) LastSeq() uint64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.seq
}

func (db *DB) Stats() map[string]int64 {
	st := db.fs.Stats().Snapshot()
	st["bytes_in"] = db.bytesIn.Load()
	st["sst_reads"] = db.sstReads.Load()
	st["last_seq"] = int64(db.LastSeq())
	db.mu.Lock()
	st["mem_bytes"] = db.mem.ApproximateBytes()
	st["l0_files"] = int64(len(db.vs.Levels[0]))
	var live int64
	for _, f := range db.vs.AllFiles() {
		live += f.Size
	}
	st["live_bytes"] = live
	db.mu.Unlock()
	return st
}

func (db *DB) WriteAmp() float64 {
	st := db.Stats()
	in := st["bytes_in"]
	if in == 0 {
		return 0
	}
	return float64(st["bytes_written"]) / float64(in)
}

func (db *DB) SpaceAmp() float64 {
	st := db.Stats()
	in := st["bytes_in"]
	if in == 0 {
		return 0
	}
	return float64(st["live_bytes"]) / float64(in)
}

// DeepestLevel returns the highest level index that currently holds at least
// one SSTable (0 when all data still sits in the memtable/L0). It is the
// measured input to the analytic write-amplification model.
func (db *DB) DeepestLevel() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	deepest := 0
	for i, lv := range db.vs.Levels {
		if len(lv) > 0 {
			deepest = i
		}
	}
	return deepest
}

func (db *DB) Version() *version.Set {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.vs.Clone()
}

func (db *DB) LiveFiles() map[uint64]bool {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.vs.FileNums()
}

func (db *DB) DirEntries() ([]string, error) {
	ents, err := db.fs.ReadDir(db.dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names, nil
}

func (db *DB) ChecksumAll() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, t := range db.tables {
		if err := t.ChecksumOK(); err != nil {
			return ErrCorrupt
		}
	}
	return nil
}

func (db *DB) allocFileLocked() uint64 {
	n := db.nextFile
	db.nextFile++
	return n
}
