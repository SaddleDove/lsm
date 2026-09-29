package engine

import (
	"fmt"
	"path"

	"lsm/internal/ikey"
	"lsm/internal/memtable"
	"lsm/internal/sstable"
	"lsm/internal/version"
	"lsm/internal/wal"
)

func (db *DB) flushLocked() error {
	if db.mem.Count() == 0 {
		return nil
	}
	imm := db.mem
	db.imm = imm
	db.mem = memtable.New()

	oldLog := db.log

	// Order (DESIGN 3 invariant B): write + rename + fsync the SST, then make
	// the MANIFEST edit durable -- and only *then* create/advertise the next
	// WAL. Creating the next WAL first (the old order) left a .log whose file
	// number was newer than the manifest's log_number whenever a crash landed
	// before the edit was durable, and gcOrphans could never collect it.
	meta, err := db.writeSST(imm, 0)
	if err != nil {
		return err
	}
	newLog := db.allocFileLocked()
	e := &version.Edit{}
	e.AddFile(version.FromSST(meta, 0))
	e.SetLogNumber(newLog)
	e.SetNextFile(db.nextFile)
	e.SetLastSeq(db.seq)
	oldest := db.snaps.oldest()
	if oldest == 0 {
		oldest = db.seq
	}
	e.SetMinSnapshot(oldest)
	if err := db.vlog.Append(e); err != nil {
		return err
	}
	db.vs.Apply(e)
	w, err := wal.OpenWriter(db.fs, db.dir, newLog, db.opt.Sync != SyncNone)
	if err != nil {
		return err
	}
	db.log = w
	rd, err := sstable.Open(db.fs, db.dir, meta.Num)
	if err != nil {
		return err
	}
	db.tables[meta.Num] = rd
	db.imm = nil
	if oldLog != nil {
		_ = oldLog.Close()
		_ = db.fs.Remove(path.Join(db.dir, wal.Filename(oldLog.Num())))
	}
	return nil
}

func (db *DB) writeSST(mem *memtable.Skiplist, level int) (sstable.Meta, error) {
	num := db.allocFileLocked()
	w, err := sstable.NewWriter(db.fs, db.dir, num, db.opt.BlockSize)
	if err != nil {
		return sstable.Meta{}, err
	}
	it := mem.NewIterator()
	it.SeekToFirst()
	for it.Valid() {
		if err := w.Add(it.Key(), it.Value()); err != nil {
			w.Abort()
			return sstable.Meta{}, err
		}
		it.Next()
	}
	meta, err := w.Finish()
	if err != nil {
		return sstable.Meta{}, err
	}
	meta.Level = level
	return meta, nil
}

func (db *DB) maybeCompactLocked() error {
	for {
		did, err := db.compactOnceLocked()
		if err != nil {
			return err
		}
		if !did {
			return nil
		}
	}
}

func (db *DB) compactOnceLocked() (bool, error) {
	level := db.pickCompactionLevel()
	if level < 0 {
		return false, nil
	}
	return true, db.compactLevelLocked(level)
}

func (db *DB) pickCompactionLevel() int {
	if len(db.vs.Levels) == 0 {
		return -1
	}
	if len(db.vs.Levels[0]) >= db.opt.L0CompactionTrig {
		return 0
	}
	base := int64(10 << 20)
	for i := 1; i < len(db.vs.Levels)-1; i++ {
		var sz int64
		for _, f := range db.vs.Levels[i] {
			sz += f.Size
		}
		limit := base
		for j := 1; j < i; j++ {
			limit *= int64(db.opt.LevelMultiplier)
		}
		if sz >= limit {
			return i
		}
	}
	return -1
}

func (db *DB) compactLevelLocked(level int) error {
	if level+1 >= len(db.vs.Levels) {
		n := make([][]version.FileMeta, level+2)
		copy(n, db.vs.Levels)
		db.vs.Levels = n
	}
	inputs := db.pickInputs(level)
	if len(inputs) == 0 {
		return nil
	}
	var lo, hi []byte
	for _, f := range inputs {
		uks := ikey.UserKey(f.Smallest)
		ukl := ikey.UserKey(f.Largest)
		if lo == nil || bytesLess(uks, lo) {
			lo = append([]byte(nil), uks...)
		}
		if hi == nil || bytesLess(hi, ukl) {
			hi = append([]byte(nil), ukl...)
		}
	}
	upper := overlapping(db.vs.Levels[level+1], lo, hi)
	all := append([]version.FileMeta(nil), inputs...)
	all = append(all, upper...)

	oldest := db.snaps.oldest()
	if oldest == 0 {
		oldest = db.seq
	}

	merged, err := db.mergeFiles(all, oldest, level+1)
	if err != nil {
		return err
	}
	e := &version.Edit{}
	for _, f := range all {
		e.DeleteFile(f.Level, f.Num)
	}
	for _, m := range merged {
		e.AddFile(version.FromSST(m, level+1))
	}
	e.SetNextFile(db.nextFile)
	e.SetLastSeq(db.seq)
	e.SetMinSnapshot(oldest)
	if err := db.vlog.Append(e); err != nil {
		return err
	}
	db.vs.Apply(e)
	for _, m := range merged {
		rd, err := sstable.Open(db.fs, db.dir, m.Num)
		if err != nil {
			return err
		}
		db.tables[m.Num] = rd
	}
	for _, f := range all {
		if rd, ok := db.tables[f.Num]; ok {
			_ = rd.Close()
			delete(db.tables, f.Num)
		}
		_ = db.fs.Remove(path.Join(db.dir, sstable.Filename(f.Num)))
	}
	return nil
}

func (db *DB) pickInputs(level int) []version.FileMeta {
	files := db.vs.Levels[level]
	if len(files) == 0 {
		return nil
	}
	if level == 0 {
		return append([]version.FileMeta(nil), files...)
	}
	return []version.FileMeta{files[0]}
}

type fileIter struct {
	rd *sstable.Reader
	it *sstable.TableIter
}

func (db *DB) mergeFiles(files []version.FileMeta, minSnap uint64, outLevel int) ([]sstable.Meta, error) {
	var its []*fileIter
	for _, f := range files {
		rd := db.tables[f.Num]
		if rd == nil {
			r, err := sstable.Open(db.fs, db.dir, f.Num)
			if err != nil {
				return nil, err
			}
			rd = r
			db.tables[f.Num] = rd
		}
		it := rd.NewIterator()
		it.SeekToFirst()
		its = append(its, &fileIter{rd: rd, it: it})
	}
	type item struct {
		key, val []byte
	}
	next := func() (item, bool) {
		best := -1
		for i, fi := range its {
			if fi.it == nil || !fi.it.Valid() {
				continue
			}
			if best < 0 || ikey.Compare(fi.it.Key(), its[best].it.Key()) < 0 {
				best = i
			}
		}
		if best < 0 {
			return item{}, false
		}
		it := item{
			key: append([]byte(nil), its[best].it.Key()...),
			val: append([]byte(nil), its[best].it.Value()...),
		}
		its[best].it.Next()
		return it, true
	}

	lastLevel := len(db.vs.Levels) - 1
	var out []sstable.Meta
	w, err := sstable.NewWriter(db.fs, db.dir, db.allocFileLocked(), db.opt.BlockSize)
	if err != nil {
		return nil, err
	}
	var lastUser []byte
	var lastSeq uint64
	var haveLast bool
	keptBelow := false
	n := 0
	for {
		it, ok := next()
		if !ok {
			break
		}
		uk := ikey.UserKey(it.key)
		seq := ikey.Seq(it.key)
		kind := ikey.KindOf(it.key)
		if haveLast && bytesEq(uk, lastUser) {
			if lastSeq <= minSnap {
				continue
			}
			if seq < minSnap && keptBelow {
				continue
			}
			if seq <= minSnap {
				keptBelow = true
			}
		} else {
			keptBelow = seq <= minSnap
			if kind == ikey.KindDelete && seq <= minSnap && outLevel >= lastLevel {
				lastUser = append(lastUser[:0], uk...)
				lastSeq = seq
				haveLast = true
				continue
			}
		}
		if err := w.Add(it.key, it.val); err != nil {
			w.Abort()
			return nil, err
		}
		n++
		lastUser = append(lastUser[:0], uk...)
		lastSeq = seq
		haveLast = true
	}
	if n > 0 {
		m, err := w.Finish()
		if err != nil {
			return nil, err
		}
		m.Level = outLevel
		out = append(out, m)
	} else {
		w.Abort()
	}
	return out, nil
}

func bytesLess(a, b []byte) bool { return string(a) < string(b) }
func bytesEq(a, b []byte) bool   { return string(a) == string(b) }

func (db *DB) DebugString() string {
	db.mu.Lock()
	defer db.mu.Unlock()
	s := fmt.Sprintf("seq=%d next=%d log=%d mem=%d\n", db.seq, db.nextFile, db.vs.LogNumber, db.mem.Count())
	for i, lv := range db.vs.Levels {
		s += fmt.Sprintf("L%d: %d files\n", i, len(lv))
		for _, f := range lv {
			s += fmt.Sprintf("  #%d size=%d\n", f.Num, f.Size)
		}
	}
	return s
}
