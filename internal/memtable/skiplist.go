package memtable

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"unsafe"

	"lsm/internal/ikey"
)

const maxHeight = 12

type node struct {
	key  []byte
	val  []byte
	next [maxHeight]unsafe.Pointer
}

func (n *node) getNext(h int) *node {
	return (*node)(atomic.LoadPointer(&n.next[h]))
}

func (n *node) setNext(h int, x *node) {
	atomic.StorePointer(&n.next[h], unsafe.Pointer(x))
}

type Skiplist struct {
	head   *node
	height atomic.Int32
	rnd    *rand.Rand
	mu     sync.Mutex
	bytes  atomic.Int64
	count  atomic.Int64
}

func New() *Skiplist {
	s := &Skiplist{
		head: &node{},
		rnd:  rand.New(rand.NewSource(1)),
	}
	s.height.Store(1)
	return s
}

func (s *Skiplist) randomHeight() int {
	h := 1
	for h < maxHeight && s.rnd.Intn(4) == 0 {
		h++
	}
	return h
}

func (s *Skiplist) findGreaterOrEqual(key []byte, prev []*node) *node {
	x := s.head
	h := int(s.height.Load()) - 1
	for {
		next := x.getNext(h)
		if next != nil && ikey.Compare(next.key, key) < 0 {
			x = next
			continue
		}
		if prev != nil {
			prev[h] = x
		}
		if h == 0 {
			return next
		}
		h--
	}
}

func (s *Skiplist) Put(key, val []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev := make([]*node, maxHeight)
	_ = s.findGreaterOrEqual(key, prev)
	h := s.randomHeight()
	ch := int(s.height.Load())
	if h > ch {
		for i := ch; i < h; i++ {
			prev[i] = s.head
		}
		s.height.Store(int32(h))
	}
	n := &node{key: append([]byte(nil), key...), val: append([]byte(nil), val...)}
	for i := 0; i < h; i++ {
		n.setNext(i, prev[i].getNext(i))
		prev[i].setNext(i, n)
	}
	s.bytes.Add(int64(len(key) + len(val) + 16))
	s.count.Add(1)
}

func (s *Skiplist) Get(user []byte, seq uint64) ([]byte, bool, bool) {
	search := ikey.Encode(user, seq, ikey.KindValue)
	n := s.findGreaterOrEqual(search, nil)
	for n != nil {
		if ikey.CompareUser(n.key, search) != 0 {
			return nil, false, false
		}
		k := ikey.KindOf(n.key)
		if k == ikey.KindDelete {
			return nil, true, true
		}
		return n.val, true, false
	}
	return nil, false, false
}

func (s *Skiplist) ApproximateBytes() int64 { return s.bytes.Load() }
func (s *Skiplist) Count() int64            { return s.count.Load() }

type Iterator struct {
	list *Skiplist
	n    *node
}

func (s *Skiplist) NewIterator() *Iterator {
	return &Iterator{list: s, n: s.head}
}

func (it *Iterator) SeekToFirst() {
	it.n = it.list.head.getNext(0)
}

func (it *Iterator) Seek(key []byte) {
	it.n = it.list.findGreaterOrEqual(key, nil)
}

func (it *Iterator) Next() {
	if it.n != nil {
		it.n = it.n.getNext(0)
	}
}

func (it *Iterator) Valid() bool { return it.n != nil }

func (it *Iterator) Key() []byte {
	if it.n == nil {
		return nil
	}
	return it.n.key
}

func (it *Iterator) Value() []byte {
	if it.n == nil {
		return nil
	}
	return it.n.val
}
