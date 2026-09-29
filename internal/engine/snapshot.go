package engine

import "sync"

type Snapshot struct {
	seq  uint64
	db   *DB
	next *Snapshot
	prev *Snapshot
}

func (s *Snapshot) Seq() uint64 { return s.seq }

type snapList struct {
	mu   sync.Mutex
	head Snapshot
}

func (l *snapList) init() {
	l.head.next = &l.head
	l.head.prev = &l.head
}

func (l *snapList) newest(seq uint64, db *DB) *Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := &Snapshot{seq: seq, db: db}
	s.next = &l.head
	s.prev = l.head.prev
	s.prev.next = s
	s.next.prev = s
	return s
}

func (l *snapList) release(s *Snapshot) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s.prev.next = s.next
	s.next.prev = s.prev
	s.next, s.prev = nil, nil
}

func (l *snapList) oldest() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.head.next == &l.head {
		return 0
	}
	return l.head.next.seq
}
