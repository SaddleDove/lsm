package memtable

import (
	"testing"

	"lsm/internal/ikey"
)

func TestSkiplistPutGet(t *testing.T) {
	s := New()
	s.Put(ikey.Encode([]byte("a"), 1, ikey.KindValue), []byte("1"))
	s.Put(ikey.Encode([]byte("a"), 2, ikey.KindValue), []byte("2"))
	s.Put(ikey.Encode([]byte("b"), 3, ikey.KindDelete), nil)
	v, ok, del := s.Get([]byte("a"), 9)
	if !ok || del || string(v) != "2" {
		t.Fatalf("got %q ok=%v del=%v", v, ok, del)
	}
	v, ok, del = s.Get([]byte("a"), 1)
	if !ok || del || string(v) != "1" {
		t.Fatalf("snap %q ok=%v del=%v", v, ok, del)
	}
	_, ok, del = s.Get([]byte("b"), 9)
	if !ok || !del {
		t.Fatalf("delete miss")
	}
}

func TestIteratorOrder(t *testing.T) {
	s := New()
	s.Put(ikey.Encode([]byte("c"), 1, ikey.KindValue), []byte("c"))
	s.Put(ikey.Encode([]byte("a"), 1, ikey.KindValue), []byte("a"))
	s.Put(ikey.Encode([]byte("b"), 1, ikey.KindValue), []byte("b"))
	it := s.NewIterator()
	it.SeekToFirst()
	var got []string
	for it.Valid() {
		got = append(got, string(ikey.UserKey(it.Key())))
		it.Next()
	}
	if len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("%v", got)
	}
}
