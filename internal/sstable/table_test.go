package sstable

import (
	"fmt"
	"testing"

	"lsm/internal/fsx"
	"lsm/internal/ikey"
)

func TestSSTableRoundtrip(t *testing.T) {
	fs := fsx.NewMemFS()
	w, err := NewWriter(fs, "/", 1, 256)
	if err != nil {
		t.Fatal(err)
	}
	n := 200
	for i := 0; i < n; i++ {
		k := ikey.Encode([]byte(fmt.Sprintf("k%04d", i)), uint64(i+1), ikey.KindValue)
		if err := w.Add(k, []byte(fmt.Sprintf("v%04d", i))); err != nil {
			t.Fatal(err)
		}
	}
	meta, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if meta.Count != n {
		t.Fatalf("count %d", meta.Count)
	}
	rd, err := Open(fs, "/", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	for i := 0; i < n; i++ {
		uk := []byte(fmt.Sprintf("k%04d", i))
		v, ok, del, err := rd.Get(uk, 1<<40)
		if err != nil || !ok || del || string(v) != fmt.Sprintf("v%04d", i) {
			t.Fatalf("i=%d v=%q ok=%v del=%v err=%v", i, v, ok, del, err)
		}
	}
	_, ok, _, err := rd.Get([]byte("missing"), 1<<40)
	if err != nil || ok {
		t.Fatalf("missing ok=%v err=%v", ok, err)
	}
}

func TestSSTableCorruptBit(t *testing.T) {
	fs := fsx.NewMemFS()
	w, _ := NewWriter(fs, "/", 1, 256)
	_ = w.Add(ikey.Encode([]byte("a"), 1, ikey.KindValue), []byte("x"))
	_, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	f, err := fs.OpenFile("/000001.sst", 2, 0644)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	_, _ = f.ReadAt(buf, 0)
	buf[0] ^= 0x01
	_, _ = f.WriteAt(buf, 0)
	_ = f.Close()
	rd, err := Open(fs, "/", 1)
	if err == nil {
		err = rd.ChecksumOK()
		if err == nil {
			_, _, _, err = rd.Get([]byte("a"), 9)
		}
	}
	if err == nil {
		t.Fatal("expected corruption error")
	}
}

func TestIterator(t *testing.T) {
	fs := fsx.NewMemFS()
	w, _ := NewWriter(fs, "/", 1, 64)
	for i := 0; i < 50; i++ {
		k := ikey.Encode([]byte(fmt.Sprintf("%02d", i)), 1, ikey.KindValue)
		_ = w.Add(k, []byte{byte(i)})
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	rd, err := Open(fs, "/", 1)
	if err != nil {
		t.Fatal(err)
	}
	it := rd.NewIterator()
	it.SeekToFirst()
	c := 0
	for it.Valid() {
		c++
		it.Next()
	}
	if c != 50 {
		t.Fatalf("got %d", c)
	}
}
