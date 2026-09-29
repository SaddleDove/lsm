package engine

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"lsm/internal/fsx"
)

func reopen(t *testing.T, fs fsx.FS, mem int64) *DB {
	t.Helper()
	db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: mem, Sync: SyncEveryWrite, L0CompactionTrig: 4})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func expectKV(t *testing.T, db *DB, k, v string) {
	t.Helper()
	got, err := db.Get([]byte(k))
	if v == "" {
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("key %s want missing got %q err=%v", k, got, err)
		}
		return
	}
	if err != nil || string(got) != v {
		t.Fatalf("key %s want %s got %q err=%v", k, v, got, err)
	}
}

// A crash while a Put's WAL record is being written must not ack or half-apply
// the batch: the previously acked key survives and the in-flight key is either
// absent or exactly right (never a wrong value).
func TestCrashWALBeforeAck(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k0"), []byte("v0")); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	base := fs.Clone()

	hits := 0
	for delta := int64(1); delta <= 40; delta++ {
		c := base.Clone()
		p := &fsx.CrashPolicy{}
		c.SetPolicy(p)
		d, err := openWith(c, 1<<20)
		if err != nil {
			continue
		}
		// Crash on one of the next few ops, i.e. during the k1 WAL append.
		p.CrashAt = c.Stats().OpCount.Load() + delta
		err = d.Put([]byte("k1"), []byte("v1"))
		_ = d.Close()
		if !p.Crashed() && !errors.Is(err, fsx.ErrCrashed) {
			continue
		}
		hits++
		cr := c.Crash()
		d2, err := openWith(cr, 1<<20)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		if v, e := d2.Get([]byte("k0")); e != nil || string(v) != "v0" {
			t.Fatalf("acked k0 lost: %q %v", v, e)
		}
		v, e := d2.Get([]byte("k1"))
		if e == nil {
			if string(v) != "v1" {
				t.Fatalf("k1 wrong value %q", v)
			}
		} else if !errors.Is(e, ErrNotFound) {
			t.Fatalf("k1 unexpected error: %v", e)
		}
		_ = d2.Close()
	}
	if hits == 0 {
		t.Fatal("no crash points exercised during the WAL append")
	}
}

func TestCrashDeterministicScan(t *testing.T) {
	const nWrites = 40
	lost := 0
	recovered := 0
	points := 0
	for crashAt := int64(1); crashAt <= 400; crashAt++ {
		fs := fsx.NewMemFS()
		p := &fsx.CrashPolicy{CrashAt: crashAt}
		fs.SetPolicy(p)
		db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 256, Sync: SyncEveryWrite, L0CompactionTrig: 3})
		acked := map[string]string{}
		if err != nil {
			continue
		}
		for i := 0; i < nWrites; i++ {
			k := fmt.Sprintf("k%02d", i)
			v := fmt.Sprintf("v%02d", i)
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				if errors.Is(err, fsx.ErrCrashed) {
					break
				}
				t.Fatalf("put: %v", err)
			}
			acked[k] = v
		}
		_ = db.Close()
		if !p.Crashed() {
			continue
		}
		points++
		crashed := fs.Crash()
		db2, err := Open(Options{Dir: "/", FS: crashed, MemtableBytes: 256, Sync: SyncEveryWrite, L0CompactionTrig: 3})
		if err != nil {
			lost++
			continue
		}
		ok := true
		for k, v := range acked {
			got, err := db2.Get([]byte(k))
			if err != nil || string(got) != v {
				ok = false
				break
			}
		}
		_ = db2.Close()
		if ok {
			recovered++
		} else {
			lost++
		}
	}
	if points < 10 {
		t.Fatalf("too few crash points hit: %d", points)
	}
	if lost > 0 {
		t.Fatalf("lost acked data at %d of %d crash points", lost, points)
	}
	t.Logf("crash points=%d recovered=%d", points, recovered)
}

func TestCrashPartialBatch(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 1 << 20, Sync: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Put([]byte("pre"), []byte("ok"))
	opsBefore := fs.Stats().OpCount.Load()
	_ = db.Close()

	for delta := int64(1); delta <= 40; delta++ {
		fs2 := fs.Clone()
		p := &fsx.CrashPolicy{CrashAt: opsBefore + delta}
		fs2.SetPolicy(p)
		db, err := Open(Options{Dir: "/", FS: fs2, MemtableBytes: 1 << 20, Sync: SyncEveryWrite})
		if err != nil {
			continue
		}
		var b WriteBatch
		b.Put([]byte("a"), []byte("1"))
		b.Put([]byte("b"), []byte("2"))
		b.Put([]byte("c"), []byte("3"))
		err = db.Write(&b, WriteOptions{Sync: true})
		_ = db.Close()
		if !errors.Is(err, fsx.ErrCrashed) && !p.Crashed() {
			continue
		}
		crashed := fs2.Crash()
		db2 := reopen(t, crashed, 1<<20)
		_, ea := db2.Get([]byte("a"))
		_, eb := db2.Get([]byte("b"))
		_, ec := db2.Get([]byte("c"))
		present := 0
		for _, e := range []error{ea, eb, ec} {
			if e == nil {
				present++
			}
		}
		_ = db2.Close()
		if present != 0 && present != 3 {
			t.Fatalf("partial batch present=%d crashAt=%d", present, opsBefore+delta)
		}
	}
}

func TestCrashFlushThenManifest(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 1 << 20, Sync: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}
	_ = db.Close()

	found := 0
	okRec := 0
	for crashAt := int64(1); crashAt <= 200; crashAt++ {
		fs2 := fs.Clone()
		p := &fsx.CrashPolicy{CrashAt: crashAt}
		fs2.SetPolicy(p)
		db, err := Open(Options{Dir: "/", FS: fs2, MemtableBytes: 1 << 20, Sync: SyncEveryWrite})
		if err != nil {
			continue
		}
		err = db.Flush()
		_ = db.Close()
		if !p.Crashed() && !errors.Is(err, fsx.ErrCrashed) {
			continue
		}
		found++
		crashed := fs2.Crash()
		db2, err := Open(Options{Dir: "/", FS: crashed, MemtableBytes: 1 << 20, Sync: SyncEveryWrite})
		if err != nil {
			continue
		}
		good := true
		for i := 0; i < 10; i++ {
			v, e := db2.Get([]byte(fmt.Sprintf("k%d", i)))
			if e != nil || string(v) != "v" {
				good = false
				break
			}
		}
		_ = db2.Close()
		if good {
			okRec++
		}
	}
	if found == 0 {
		t.Fatal("no flush crash points")
	}
	if okRec != found {
		t.Fatalf("flush crash lost data: %d/%d", okRec, found)
	}
}

// Two recoveries of the same state must agree on logical state and file set
// (I5). Delegates to the crash-based implementation.
func TestDoubleRecovery(t *testing.T) {
	TestRecoveryIdempotent(t)
}

func TestCrashTornWAL(t *testing.T) {
	fs := fsx.NewMemFS()
	p := &fsx.CrashPolicy{TearLast: true, TearBytes: 5}
	fs.SetPolicy(p)
	db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 1 << 20, Sync: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Put([]byte("a"), []byte("1"))
	_ = db.Put([]byte("b"), []byte("2"))
	_ = db.Close()
	crashed := fs.Crash()
	db2, err := Open(Options{Dir: "/", FS: crashed, MemtableBytes: 1 << 20, Sync: SyncEveryWrite})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	expectKV(t, db2, "a", "1")
}

func TestI1I8InvariantsSmall(t *testing.T) {
	fs := fsx.NewMemFS()
	db := reopen(t, fs, 200)
	for i := 0; i < 60; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if err := db.Put(k, k); err != nil {
			t.Fatal(err)
		}
		if i%7 == 0 {
			_ = db.Delete(k)
		}
	}
	snap := db.Snapshot()
	_ = db.Put([]byte("k001"), []byte("newer"))
	v, err := snap.Get([]byte("k001"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte("k001")) {
		t.Fatalf("snapshot saw %q, want pre-snapshot value k001", v)
	}
	snap.Release()
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	if err := db.ChecksumAll(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	db = reopen(t, fs, 200)
	defer db.Close()
	if _, err := db.Get([]byte("k007")); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted key resurrected")
	}
	live := db.LiveFiles()
	ents, _ := db.DirEntries()
	for _, name := range ents {
		var n uint64
		if _, e := fmt.Sscanf(name, "%d.sst", &n); e == nil {
			if !live[n] {
				t.Fatalf("leak %s", name)
			}
		}
	}
}
