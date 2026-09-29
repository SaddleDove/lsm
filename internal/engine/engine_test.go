package engine

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"lsm/internal/fsx"
	"lsm/internal/wal"
)

func openMem(t *testing.T, memBytes int64) (*DB, *fsx.MemFS) {
	t.Helper()
	fs := fsx.NewMemFS()
	db, err := Open(Options{
		Dir:              "/",
		FS:               fs,
		MemtableBytes:    memBytes,
		Sync:             SyncEveryWrite,
		L0CompactionTrig: 4,
		MaxLevel:         7,
	})
	if err != nil {
		t.Fatal(err)
	}
	return db, fs
}

func TestPutGet(t *testing.T) {
	db, _ := openMem(t, 1<<20)
	defer db.Close()
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("a"))
	if err != nil || string(v) != "1" {
		t.Fatalf("%q %v", v, err)
	}
}

func TestDeleteNoResurrection(t *testing.T) {
	db, fs := openMem(t, 256)
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want not found after delete, got %v", err)
	}
	_ = db.Close()
	db2, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 256, Sync: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, err := db2.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("resurrected after reopen: %v", err)
	}
}

// A WriteBatch larger than the WAL payload cap must be rejected loudly: the
// caller sees an error and the batch is not applied or acked.
func TestWriteRejectsOversizeBatch(t *testing.T) {
	old := wal.MaxPayload
	wal.MaxPayload = 256
	defer func() { wal.MaxPayload = old }()

	db, _ := openMem(t, 1<<20)
	defer db.Close()
	before := db.LastSeq()
	if err := db.Put([]byte("k"), bytes.Repeat([]byte("x"), 512)); err == nil {
		t.Fatal("expected oversize write to fail")
	}
	if got := db.LastSeq(); got != before {
		t.Fatalf("sequence advanced on rejected write: %d -> %d", before, got)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected write became visible: %v", err)
	}
}

func TestBatchAtomic(t *testing.T) {
	db, _ := openMem(t, 1<<20)
	defer db.Close()
	var b WriteBatch
	b.Put([]byte("a"), []byte("1"))
	b.Put([]byte("b"), []byte("2"))
	b.Delete([]byte("c"))
	if err := db.Write(&b, WriteOptions{Sync: true}); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("a"))
	if err != nil || string(v) != "1" {
		t.Fatal(err)
	}
	v, err = db.Get([]byte("b"))
	if err != nil || string(v) != "2" {
		t.Fatal(err)
	}
}

func TestFlushAndReopen(t *testing.T) {
	db, fs := openMem(t, 64)
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if err := db.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	db2, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 64, Sync: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		v, err := db2.Get(k)
		if err != nil || !bytes.Equal(v, k) {
			t.Fatalf("i=%d v=%q err=%v", i, v, err)
		}
	}
}

func TestSnapshotIsolation(t *testing.T) {
	db, _ := openMem(t, 1<<20)
	defer db.Close()
	_ = db.Put([]byte("k"), []byte("v1"))
	snap := db.Snapshot()
	defer snap.Release()
	_ = db.Put([]byte("k"), []byte("v2"))
	v, err := snap.Get([]byte("k"))
	if err != nil || string(v) != "v1" {
		t.Fatalf("snap %q %v", v, err)
	}
	v, err = db.Get([]byte("k"))
	if err != nil || string(v) != "v2" {
		t.Fatalf("cur %q %v", v, err)
	}
}

func TestSeqMonotonic(t *testing.T) {
	db, _ := openMem(t, 1<<20)
	defer db.Close()
	var last uint64
	for i := 0; i < 20; i++ {
		if err := db.Put([]byte("k"), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		s := db.LastSeq()
		if s <= last {
			t.Fatalf("seq not monotonic %d -> %d", last, s)
		}
		last = s
	}
}

// TestRecoveryIdempotent covers I5 (DESIGN 2.1 / 52): recovering the same state
// twice must produce the same logical state *and* the same file set.
func TestRecoveryIdempotent(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 128)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		_ = db.Put([]byte(fmt.Sprintf("%d", i)), []byte("x"))
	}
	_ = db.Flush()
	// Leave later writes only in the WAL so recovery also replays a log.
	for i := 30; i < 40; i++ {
		_ = db.Put([]byte(fmt.Sprintf("%d", i)), []byte("x"))
	}
	_ = db.Close()

	crashed := fs.Crash()

	verify := func(name string) (uint64, string) {
		t.Helper()
		clone := crashed.Clone()
		d := reopen(t, clone, 128)
		defer d.Close()
		for i := 0; i < 40; i++ {
			v, err := d.Get([]byte(fmt.Sprintf("%d", i)))
			if err != nil || string(v) != "x" {
				t.Fatalf("%s recovery i=%d %v", name, i, err)
			}
		}
		return d.LastSeq(), fileSetHash(t, clone)
	}
	s1, h1 := verify("first")
	s2, h2 := verify("second")
	if s1 != s2 {
		t.Fatalf("recovered seq differs: %d vs %d", s1, s2)
	}
	if h1 != h2 {
		t.Fatalf("file set differs across recoveries:\n%s\n%s", h1, h2)
	}
}

func TestCompactionKeepsData(t *testing.T) {
	db, _ := openMem(t, 256)
	defer db.Close()
	db.opt.L0CompactionTrig = 2
	n := 200
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		if err := db.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		v, err := db.Get(k)
		if err != nil || !bytes.Equal(v, k) {
			t.Fatalf("i=%d v=%q err=%v", i, v, err)
		}
	}
}

func TestOverwriteAfterFlush(t *testing.T) {
	db, fs := openMem(t, 64)
	_ = db.Put([]byte("k"), []byte("v1"))
	_ = db.Flush()
	_ = db.Put([]byte("k"), []byte("v2"))
	_ = db.Flush()
	v, err := db.Get([]byte("k"))
	if err != nil || string(v) != "v2" {
		t.Fatalf("%q %v", v, err)
	}
	_ = db.Close()
	db2, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 64, Sync: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	v, err = db2.Get([]byte("k"))
	if err != nil || string(v) != "v2" {
		t.Fatalf("reopen %q %v", v, err)
	}
}

func TestSilentCorruptionDetected(t *testing.T) {
	db, fs := openMem(t, 32)
	for i := 0; i < 40; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%02d", i)), []byte("v"))
	}
	_ = db.Flush()
	_ = db.Close()
	ents, _ := fs.ReadDir("/")
	var sst string
	for _, e := range ents {
		if len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".sst" {
			sst = e.Name()
			break
		}
	}
	if sst == "" {
		t.Fatal("no sst")
	}
	f, err := fs.OpenFile("/"+sst, 2, 0644)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	_, _ = f.ReadAt(buf, 8)
	buf[0] ^= 0x01
	_, _ = f.WriteAt(buf, 8)
	_ = f.Close()
	db2, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 32, Sync: SyncEveryWrite})
	if err != nil {
		return
	}
	defer db2.Close()
	err = db2.ChecksumAll()
	if err == nil {
		_, err = db2.Get([]byte("k00"))
		if err == nil {
			for i := 0; i < 40; i++ {
				_, e2 := db2.Get([]byte(fmt.Sprintf("k%02d", i)))
				if errors.Is(e2, ErrCorrupt) {
					return
				}
			}
		}
	}
	if !errors.Is(err, ErrCorrupt) && err == nil {
		t.Fatal("expected corruption detection")
	}
}

// checkNoGarbage asserts the directory contains only version-referenced SSTs,
// the live WAL, the live MANIFEST and CURRENT/LOCK (DESIGN 2.1 / 55).
func checkNoGarbage(t *testing.T, db *DB) {
	t.Helper()
	live := db.vs.FileNums()
	liveLog := db.vs.LogNumber
	liveMan := db.vs.Manifest
	ents, _ := db.DirEntries()
	for _, name := range ents {
		var n uint64
		if _, err := fmt.Sscanf(name, "%d.sst", &n); err == nil {
			if !live[n] {
				t.Fatalf("orphan sst %s", name)
			}
			continue
		}
		if _, err := fmt.Sscanf(name, "%d.log", &n); err == nil {
			if n != liveLog {
				t.Fatalf("stray log %s (live log=%d)", name, liveLog)
			}
			continue
		}
		if _, err := fmt.Sscanf(name, "MANIFEST-%d", &n); err == nil {
			if n != liveMan {
				t.Fatalf("stray manifest %s (live=%d)", name, liveMan)
			}
			continue
		}
		if name == "CURRENT" || name == "LOCK" {
			continue
		}
		t.Fatalf("unexpected file %s", name)
	}
}

func TestNoFileLeak(t *testing.T) {
	// Clean reopen -> zero garbage.
	fs := fsx.NewMemFS()
	db := reopen(t, fs, 128)
	for i := 0; i < 80; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("xxxxxxxx"))
	}
	_ = db.CompactAll()
	_ = db.Close()
	db2, err := openWith(fs, 128)
	if err != nil {
		t.Fatal(err)
	}
	checkNoGarbage(t, db2)
	_ = db2.Close()

	// Dirty reopen (power loss, no clean Close) after stray files appear ->
	// still zero garbage, and no data loss.
	fs3 := fsx.NewMemFS()
	db3 := reopen(t, fs3, 128)
	for i := 0; i < 80; i++ {
		_ = db3.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("xxxxxxxx"))
	}
	_ = db3.CompactAll()
	for _, name := range []string{"/000900.sst.tmp", "/000900.log", "/CURRENT.tmp"} {
		f, err := fs3.OpenFile(name, 0x42, 0644)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.Write([]byte("garbage"))
		_ = f.Close()
	}
	crashed := fs3.Crash()
	db4, err := openWith(crashed, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer db4.Close()
	checkNoGarbage(t, db4)
	for i := 0; i < 80; i++ {
		if _, err := db4.Get([]byte(fmt.Sprintf("k%04d", i))); err != nil {
			t.Fatalf("lost k%04d: %v", i, err)
		}
	}
}
