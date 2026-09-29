package engine

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"lsm/internal/fsx"
	"lsm/internal/version"
)

// I7 (DESIGN 2.1 / 54): flipping any single bit in an SSTable, MANIFEST or WAL
// must not yield silently wrong data. For SSTable and MANIFEST the engine must
// surface ErrCorruption; for the WAL the torn-tail rule (DESIGN 4.2) means the
// damaged record and everything after it are dropped rather than returned.
func TestI7BitFlipAcrossFiles(t *testing.T) {
	build := func(sst bool) (*fsx.MemFS, map[string]string) {
		fs := fsx.NewMemFS()
		db, err := openWith(fs, 1024)
		if err != nil {
			t.Fatal(err)
		}
		ref := map[string]string{}
		for i := 0; i < 60; i++ {
			k := fmt.Sprintf("k%03d", i)
			v := fmt.Sprintf("v%03d", i)
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			ref[k] = v
			if sst && i%20 == 19 {
				if err := db.Flush(); err != nil {
					t.Fatal(err)
				}
			}
		}
		if sst {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		_ = db.Close()
		return fs, ref
	}

	fileOf := func(fs fsx.FS, suffix string) string {
		ents, _ := fs.ReadDir("/")
		for _, e := range ents {
			if strings.HasSuffix(e.Name(), suffix) {
				return "/" + e.Name()
			}
		}
		t.Fatalf("no %s file", suffix)
		return ""
	}
	manifestOf := func(fs fsx.FS) string {
		ents, _ := fs.ReadDir("/")
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), "MANIFEST-") {
				return "/" + e.Name()
			}
		}
		t.Fatal("no MANIFEST file")
		return ""
	}
	sizeOf := func(fs fsx.FS, name string) int64 {
		st, err := fs.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		return st.Size()
	}

	// --- SSTable ---
	{
		fs, _ := build(true)
		name := fileOf(fs, ".sst")
		size := sizeOf(fs, name)
		const samples = 160
		for i := 0; i < samples; i++ {
			off := int64(i) * size / samples
			c := fs.Clone()
			flipBit(t, c, name, off)
			db, err := openWith(c, 1024)
			if err != nil {
				continue // corruption surfaced while opening the table
			}
			err = db.ChecksumAll()
			_ = db.Close()
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("sst bit flip at offset %d not reported as corruption (err=%v)", off, err)
			}
		}
	}

	// --- MANIFEST ---
	{
		fs, _ := build(true)
		name := manifestOf(fs)
		size := sizeOf(fs, name)
		const samples = 160
		for i := 0; i < samples; i++ {
			off := int64(i) * size / samples
			c := fs.Clone()
			flipBit(t, c, name, off)
			db, err := openWith(c, 1024)
			if db != nil {
				_ = db.Close()
			}
			if err == nil {
				t.Fatalf("manifest bit flip at offset %d not detected", off)
			}
			if !errors.Is(err, version.ErrCorruptManifest) {
				t.Fatalf("manifest bit flip at offset %d: want ErrCorruptManifest, got %v", off, err)
			}
		}
	}

	// --- WAL ---
	{
		fs, ref := build(false)
		name := fileOf(fs, ".log")
		size := sizeOf(fs, name)
		const samples = 160
		for i := 0; i < samples; i++ {
			off := int64(i) * size / samples
			c := fs.Clone()
			flipBit(t, c, name, off)
			db, err := openWith(c, 1024)
			if err != nil {
				t.Fatalf("wal bit flip at %d made the db unopenable (WAL damage must truncate, not error): %v", off, err)
			}
			for k, v := range ref {
				got, gerr := db.Get([]byte(k))
				if gerr == nil {
					if string(got) != v {
						_ = db.Close()
						t.Fatalf("wal bit flip at %d produced WRONG value for %s: %q want %q", off, k, got, v)
					}
				} else if !errors.Is(gerr, ErrNotFound) {
					_ = db.Close()
					t.Fatalf("wal bit flip at %d: unexpected error for %s: %v", off, k, gerr)
				}
			}
			_ = db.Close()
		}
	}
}

func flipBit(t *testing.T, fs fsx.FS, name string, off int64) {
	t.Helper()
	f, err := fs.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, off); err != nil {
		f.Close()
		t.Fatalf("read %s@%d: %v", name, off, err)
	}
	buf[0] ^= 0x01
	if _, err := f.WriteAt(buf, off); err != nil {
		f.Close()
		t.Fatal(err)
	}
	_ = f.Close()
}
