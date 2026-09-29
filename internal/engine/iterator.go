package engine

import (
	"bytes"

	"lsm/internal/ikey"
	"lsm/internal/memtable"
	"lsm/internal/sstable"
	"lsm/internal/version"
)

// scanSource is one ordered stream of internal keys (a memtable or an
// SSTable). All sources yield internal keys in ikey.Compare order.
type scanSource interface {
	SeekToFirst()
	Seek(key []byte)
	Valid() bool
	Key() []byte
	Value() []byte
	Next()
	Err() error
}

// memSource adapts a memtable iterator to scanSource (memtables are in memory
// and therefore never report an I/O error).
type memSource struct{ *memtable.Iterator }

func (memSource) Err() error { return nil }

// KV is one user-visible key/value pair produced by a Scan.
type KV struct {
	Key   []byte
	Value []byte
}

// Iterator is a materialised, forward-only view over a range. Because the
// engine has a single writer and no background threads, Scan() runs the merge
// to completion under the DB lock, so the returned iterator is a stable
// snapshot even if the caller keeps mutating the DB afterwards.
type Iterator struct {
	kvs []KV
	i   int
	err error
}

func (it *Iterator) Valid() bool {
	return it.err == nil && it.i >= 0 && it.i < len(it.kvs)
}

func (it *Iterator) Next() { it.i++ }

func (it *Iterator) Err() error { return it.err }

// Key returns the current user key. The slice is owned by the iterator and must
// not be mutated.
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.kvs[it.i].Key
}

// Value returns the current value (nil for a key that is absent, which cannot
// happen because absent keys are suppressed during the merge).
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.kvs[it.i].Value
}

// mergingIterator performs a k-way merge of active memtable, immutable
// memtable and every overlapping SSTable, applying snapshot filtering and
// tombstone masking (DESIGN 6, "Scan / MergingIterator").
type mergingIterator struct {
	srcs []scanSource
	seq  uint64
	end  []byte

	key   []byte
	val   []byte
	valid bool
	err   error
}

func (m *mergingIterator) add(src scanSource, start []byte) {
	if start == nil {
		src.SeekToFirst()
	} else {
		src.Seek(start)
	}
	m.srcs = append(m.srcs, src)
}

// newMergingIterator builds the source list under the caller's lock. Sources
// are added newest-first among equal internal keys: active memtable, immutable
// memtable, L0 files newest->oldest, then L1..L6.
func newMergingIterator(mem, imm *memtable.Skiplist, vs *version.Set, tables map[uint64]*sstable.Reader, seq uint64, start, end []byte) *mergingIterator {
	m := &mergingIterator{seq: seq, end: end}
	var startIK []byte
	if start != nil {
		startIK = ikey.Encode(start, ikey.MaxSeq(), ikey.KindValue)
	}
	if mem != nil {
		m.add(memSource{mem.NewIterator()}, startIK)
	}
	if imm != nil {
		m.add(memSource{imm.NewIterator()}, startIK)
	}
	for level, files := range vs.Levels {
		cands := files
		if level == 0 {
			cands = append([]version.FileMeta(nil), files...)
			sortByNumDesc(cands)
		}
		for _, f := range cands {
			if !inRange(f, start, end) {
				continue
			}
			rd := tables[f.Num]
			if rd == nil {
				continue
			}
			m.add(rd.NewIterator(), startIK)
		}
	}
	return m
}

func sortByNumDesc(files []version.FileMeta) {
	for i := 1; i < len(files); i++ {
		for j := i; j > 0 && files[j-1].Num < files[j].Num; j-- {
			files[j-1], files[j] = files[j], files[j-1]
		}
	}
}

// inRange reports whether a file can contain a user key in [start,end).
func inRange(f version.FileMeta, start, end []byte) bool {
	if start != nil && bytes.Compare(ikey.UserKey(f.Largest), start) < 0 {
		return false
	}
	if end != nil && bytes.Compare(ikey.UserKey(f.Smallest), end) >= 0 {
		return false
	}
	return true
}

func (m *mergingIterator) minSource() int {
	best := -1
	for i, s := range m.srcs {
		if !s.Valid() {
			if e := s.Err(); e != nil && m.err == nil {
				m.err = e
			}
			continue
		}
		if best < 0 || ikey.Compare(s.Key(), m.srcs[best].Key()) < 0 {
			best = i
		}
	}
	return best
}

// Next advances to the next user-visible key. Entries newer than the snapshot
// are skipped; tombstones mask older versions of the same key and are never
// emitted themselves.
func (m *mergingIterator) Next() {
	m.valid = false
	for {
		first := m.minSource()
		if first < 0 {
			return
		}
		uk := append([]byte(nil), ikey.UserKey(m.srcs[first].Key())...)
		if m.end != nil && bytes.Compare(uk, m.end) >= 0 {
			return
		}
		var out []byte
		found := false
		isDel := false
		for {
			j := m.minSource()
			if j < 0 {
				break
			}
			k := m.srcs[j].Key()
			if bytes.Compare(ikey.UserKey(k), uk) != 0 {
				break
			}
			if !found && ikey.Seq(k) <= m.seq {
				found = true
				if ikey.KindOf(k) == ikey.KindDelete {
					isDel = true
				} else {
					out = append([]byte(nil), m.srcs[j].Value()...)
				}
			}
			m.srcs[j].Next()
		}
		if found && !isDel {
			m.key = uk
			m.val = out
			m.valid = true
			return
		}
	}
}

func (m *mergingIterator) Valid() bool   { return m.valid }
func (m *mergingIterator) Key() []byte   { return m.key }
func (m *mergingIterator) Value() []byte { return m.val }
func (m *mergingIterator) Err() error    { return m.err }

// Scan returns every user key in [start,end) visible at the current sequence.
// start == nil means unbounded below; end == nil means unbounded above. The
// bound is start-inclusive, end-exclusive.
func (db *DB) Scan(start, end []byte) (*Iterator, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	return db.scanLocked(start, end, db.seq)
}

// Scan on a Snapshot scans as of the snapshot's sequence, so later writes and
// concurrent compaction cannot leak into the result.
func (s *Snapshot) Scan(start, end []byte) (*Iterator, error) {
	s.db.mu.Lock()
	defer s.db.mu.Unlock()
	if s.db.closed {
		return nil, ErrClosed
	}
	return s.db.scanLocked(start, end, s.seq)
}

func (db *DB) scanLocked(start, end []byte, seq uint64) (*Iterator, error) {
	mi := newMergingIterator(db.mem, db.imm, db.vs, db.tables, seq, start, end)
	var kvs []KV
	for {
		mi.Next()
		if !mi.Valid() {
			break
		}
		kvs = append(kvs, KV{
			Key:   append([]byte(nil), mi.Key()...),
			Value: append([]byte(nil), mi.Value()...),
		})
	}
	if err := mi.Err(); err != nil {
		return nil, err
	}
	return &Iterator{kvs: kvs}, nil
}
