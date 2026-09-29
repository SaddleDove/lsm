package version

import (
	"bytes"
	"testing"

	"lsm/internal/fsx"
)

func TestEditRoundtrip(t *testing.T) {
	e := &Edit{}
	e.SetLogNumber(3)
	e.SetNextFile(9)
	e.SetLastSeq(42)
	e.SetMinSnapshot(7)
	e.AddFile(FileMeta{Num: 4, Level: 1, Size: 100, Smallest: []byte("a"), Largest: []byte("z"), Count: 3})
	e.DeleteFile(0, 2)
	raw := Encode(e)
	got, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if *got.LogNumber != 3 || *got.LastSeq != 42 || len(got.Add) != 1 || len(got.Del) != 1 {
		t.Fatalf("%+v", got)
	}
	rec := EncodeRecord(e)
	g2, err := DecodeRecord(bytes.NewReader(rec))
	if err != nil {
		t.Fatal(err)
	}
	if *g2.NextFile != 9 {
		t.Fatal(g2)
	}
}

func TestManifestCurrent(t *testing.T) {
	fs := fsx.NewMemFS()
	set := NewSet(7)
	set.LogNumber = 1
	set.NextFile = 4
	set.LastSeq = 10
	l, err := CreateManifest(fs, "/", 1, set)
	if err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	name, err := ReadCurrent(fs, "/")
	if err != nil || name != "MANIFEST-000001" {
		t.Fatalf("%q %v", name, err)
	}
	l2, set2, err := OpenManifest(fs, "/")
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	if set2.LastSeq != 10 || set2.NextFile < 4 {
		t.Fatalf("%+v", set2)
	}
}

func TestApplyAddDel(t *testing.T) {
	s := NewSet(7)
	e := &Edit{}
	e.AddFile(FileMeta{Num: 1, Level: 0, Size: 10, Smallest: []byte("a"), Largest: []byte("b")})
	s.Apply(e)
	if len(s.Levels[0]) != 1 {
		t.Fatal(s.Levels[0])
	}
	e2 := &Edit{}
	e2.DeleteFile(0, 1)
	s.Apply(e2)
	if len(s.Levels[0]) != 0 {
		t.Fatal("not deleted")
	}
}
