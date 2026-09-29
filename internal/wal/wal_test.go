package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"lsm/internal/fsx"
	"lsm/internal/ikey"
)

func TestWALRoundtrip(t *testing.T) {
	fs := fsx.NewMemFS()
	_ = fs.MkdirAll("/", 0755)
	w, err := OpenWriter(fs, "/", 1, true)
	if err != nil {
		t.Fatal(err)
	}
	recs := []Record{
		{Seq: 1, Kind: ikey.KindValue, Key: []byte("a"), Val: []byte("1")},
		{Seq: 2, Kind: ikey.KindDelete, Key: []byte("b")},
	}
	if err := w.Append(recs); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	rd, err := OpenReader(fs, "/", 1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := rd.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].Val) != "1" || got[1].Kind != ikey.KindDelete {
		t.Fatalf("%+v", got)
	}
	if _, err := rd.Next(); err != io.EOF {
		t.Fatalf("want eof got %v", err)
	}
}

func TestWALTornTail(t *testing.T) {
	fs := fsx.NewMemFS()
	w, err := OpenWriter(fs, "/", 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]Record{{Seq: 1, Kind: ikey.KindValue, Key: []byte("a"), Val: []byte("1")}}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	f, err := fs.OpenFile("/000001.log", 2, 0644)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := f.Stat()
	_ = f.Truncate(st.Size() - 3)
	_ = f.Close()
	rd, err := OpenReader(fs, "/", 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = rd.Next()
	if err != ErrTorn && err != io.EOF {
		t.Fatalf("want torn/eof got %v", err)
	}
}

func TestWriterRejectsOversizePayload(t *testing.T) {
	old := MaxPayload
	MaxPayload = 64
	defer func() { MaxPayload = old }()

	fs := fsx.NewMemFS()
	w, err := OpenWriter(fs, "/", 1, true)
	if err != nil {
		t.Fatal(err)
	}
	big := []Record{{Seq: 1, Kind: ikey.KindValue, Key: []byte("k"), Val: make([]byte, 128)}}
	if err := w.Append(big); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("want ErrTooLarge got %v", err)
	}
	// A rejected append must not advance the write offset (nothing acked).
	if w.Off() != 0 {
		t.Fatalf("offset advanced to %d after rejected append", w.Off())
	}
	// And the writer stays usable for a small record.
	if err := w.Append([]Record{{Seq: 2, Kind: ikey.KindValue, Key: []byte("k"), Val: []byte("v")}}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
}

func TestReaderRejectsOversizeHeader(t *testing.T) {
	old := MaxPayload
	MaxPayload = 64
	defer func() { MaxPayload = old }()

	fs := fsx.NewMemFS()
	f, err := fs.OpenFile("/000001.log", 0x42 /*O_RDWR|O_CREATE*/, 0644)
	if err != nil {
		t.Fatal(err)
	}
	hdr := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(MaxPayload+1)) // declared length over the cap
	if _, err := f.Write(hdr); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	rd, err := OpenReader(fs, "/", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	if _, err := rd.Next(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt got %v", err)
	}
}

func TestReplaySkipsOldSeq(t *testing.T) {
	fs := fsx.NewMemFS()
	w, _ := OpenWriter(fs, "/", 1, true)
	_ = w.Append([]Record{
		{Seq: 1, Kind: ikey.KindValue, Key: []byte("a"), Val: []byte("old")},
		{Seq: 2, Kind: ikey.KindValue, Key: []byte("a"), Val: []byte("new")},
	})
	_ = w.Close()
	var seen []Record
	last, _, err := Replay(fs, "/", 1, func(r Record) error {
		seen = append(seen, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if last != 2 || len(seen) != 1 || string(seen[0].Val) != "new" {
		t.Fatalf("last=%d seen=%+v", last, seen)
	}
}
