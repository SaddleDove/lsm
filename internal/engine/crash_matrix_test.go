package engine

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"lsm/internal/fsx"
	"lsm/internal/version"
	"lsm/internal/wal"
)

func openWith(fs fsx.FS, mem int64) (*DB, error) {
	return Open(Options{Dir: "/", FS: fs, MemtableBytes: mem, Sync: SyncEveryWrite, L0CompactionTrig: 4, MaxLevel: 7})
}

func mustPut(t *testing.T, db *DB, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		if err := db.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}
}

func checkAll(t *testing.T, db *DB, n int, deleted map[int]bool) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%04d", i))
		v, err := db.Get(k)
		if deleted[i] {
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted %d present %q %v", i, v, err)
			}
			continue
		}
		if err != nil || string(v) != string(k) {
			t.Fatalf("k%04d got %q err=%v", i, v, err)
		}
	}
}

// orphanSSTs returns .sst files present on disk but not referenced by the
// durable manifest (i.e. a rename that made it to disk before the MANIFEST edit
// was durable).
func orphanSSTs(fs fsx.FS) []string {
	l, set, err := version.OpenManifest(fs, "/")
	if err != nil {
		return nil
	}
	_ = l.Close()
	live := set.FileNums()
	ents, _ := fs.ReadDir("/")
	var out []string
	for _, e := range ents {
		var n uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.sst", &n); err == nil && !live[n] {
			out = append(out, e.Name())
		}
	}
	return out
}

// nonLiveLogs returns .log files that are not the manifest's current log_number.
func nonLiveLogs(fs fsx.FS) []string {
	l, set, err := version.OpenManifest(fs, "/")
	if err != nil {
		return nil
	}
	_ = l.Close()
	ents, _ := fs.ReadDir("/")
	var out []string
	for _, e := range ents {
		var n uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.log", &n); err == nil && n != set.LogNumber {
			out = append(out, e.Name())
		}
	}
	return out
}

// assertManifestFilesExist checks the invariant "old files are only deleted
// after the edit that removes them is durable": every file the durable
// manifest references must still be on disk.
func assertManifestFilesExist(t *testing.T, fs fsx.FS) {
	t.Helper()
	l, set, err := version.OpenManifest(fs, "/")
	if err != nil {
		t.Fatalf("open manifest after crash: %v", err)
	}
	_ = l.Close()
	for _, f := range set.AllFiles() {
		name := fmt.Sprintf("/%06d.sst", f.Num)
		if _, err := fs.Stat(name); err != nil {
			t.Fatalf("manifest references %s but it is missing: %v", name, err)
		}
	}
}

// c1: a torn WAL tail truncates exactly the torn record; every fully persisted
// record before it still replays.
func TestC1WALTornTail(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, 5)
	_ = db.Close()
	nums, err := wal.List(fs, "/")
	if err != nil || len(nums) == 0 {
		t.Fatalf("logs %v %v", nums, err)
	}
	name := fmt.Sprintf("/%s", wal.Filename(nums[len(nums)-1]))
	f, err := fs.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	st, _ := f.Stat()
	if st.Size() < 10 {
		t.Fatal("log too small")
	}
	_ = f.Truncate(st.Size() - 5)
	_ = f.Close()

	db, err = openWith(fs, 1<<20)
	if err != nil {
		t.Fatalf("reopen after torn tail: %v", err)
	}
	defer db.Close()
	// Records 0..3 were complete before the torn tail -> must replay.
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("k%04d", i)
		v, err := db.Get([]byte(k))
		if err != nil || string(v) != k {
			t.Fatalf("prior record %s lost: %q %v", k, v, err)
		}
	}
	// The torn record (and everything after) is dropped, not half-applied.
	if v, err := db.Get([]byte("k0004")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("torn record should be absent, got %q %v", v, err)
	}
}

func TestC2AckedWALSurvives(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, 8)
	_ = db.Close()
	crashed := fs.Crash()
	db, err = openWith(crashed, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checkAll(t, db, 8, nil)
}

func TestC3PartialBatchNeverVisible(t *testing.T) {
	TestCrashPartialBatch(t)
}

// c4: crash while a flush temp file is being written -> data is still in the WAL.
func TestC4FlushTmpBeforeRename(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, 12)
	_ = db.Close()
	hits := 0
	for at := int64(1); at <= 80; at++ {
		fs2 := fs.Clone()
		p := &fsx.CrashPolicy{CrashAt: at}
		fs2.SetPolicy(p)
		db, err := openWith(fs2, 1<<20)
		if err != nil {
			continue
		}
		err = db.Flush()
		_ = db.Close()
		if !p.Crashed() && !errors.Is(err, fsx.ErrCrashed) {
			continue
		}
		hits++
		crashed := fs2.Crash()
		db2, err := openWith(crashed, 1<<20)
		if err != nil {
			t.Fatalf("reopen after flush crash: %v", err)
		}
		checkAll(t, db2, 12, nil)
		assertManifestFilesExist(t, crashed)
		_ = db2.Close()
	}
	if hits == 0 {
		t.Fatal("no crash points during flush")
	}
}

// c5: an SSTable that reached disk (rename durable) but is not yet in the
// MANIFEST must be ignored (not served) and collected on the next open. This is
// now a distinct fault from c4 (which crashes before rename).
func TestC5RenameSSTBeforeMANIFEST(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, 12)
	_ = db.Close()
	hits := 0
	gcHits := 0
	for at := int64(1); at <= 200; at++ {
		fs2 := fs.Clone()
		p := &fsx.CrashPolicy{CrashAt: at}
		fs2.SetPolicy(p)
		db, err := openWith(fs2, 1<<20)
		if err != nil {
			continue
		}
		err = db.Flush()
		_ = db.Close()
		if !p.Crashed() && !errors.Is(err, fsx.ErrCrashed) {
			continue
		}
		hits++
		crashed := fs2.Crash()
		orphans := orphanSSTs(crashed)
		db2, err := openWith(crashed, 1<<20)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		checkAll(t, db2, 12, nil)
		live := db2.LiveFiles()
		ents, _ := db2.DirEntries()
		for _, name := range ents {
			var n uint64
			if _, err := fmt.Sscanf(name, "%d.sst", &n); err == nil && !live[n] {
				t.Fatalf("orphan sst survived GC: %s", name)
			}
		}
		_ = db2.Close()
		if len(orphans) > 0 {
			gcHits++
		}
	}
	if hits == 0 {
		t.Fatal("no crash points during flush")
	}
	if gcHits == 0 {
		t.Fatal("never exercised the rename-durable / edit-not-durable window (no orphan SST to GC)")
	}
}

// c6: MANIFEST edit durable, old WAL not yet dropped -> replay must skip the
// already-persisted entries (via last_sequence) and serve from the SST.
func TestC6MANIFESTThenOldWAL(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, 10)
	_ = db.Close()
	hits := 0
	oldWALHits := 0
	for at := int64(1); at <= 200; at++ {
		fs2 := fs.Clone()
		p := &fsx.CrashPolicy{CrashAt: at}
		fs2.SetPolicy(p)
		db, err := openWith(fs2, 1<<20)
		if err != nil {
			continue
		}
		err = db.Flush()
		_ = db.Close()
		if !p.Crashed() && !errors.Is(err, fsx.ErrCrashed) {
			continue
		}
		hits++
		crashed := fs2.Crash()
		if len(nonLiveLogs(crashed)) > 0 {
			oldWALHits++
		}
		db2, err := openWith(crashed, 1<<20)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		checkAll(t, db2, 10, nil)
		if got := db2.LastSeq(); got == 0 {
			t.Fatal("recovered sequence not advanced")
		}
		_ = db2.Close()
	}
	if hits == 0 {
		t.Fatal("no crash points during flush")
	}
	if oldWALHits == 0 {
		t.Fatal("never exercised the manifest-durable / old-WAL-not-dropped window")
	}
}

// c7: crash mid-compaction -> the old input files remain until the edit that
// removes them is durable; recovery is always correct and never references a
// deleted file.
func TestC7CompactionBeforeMANIFEST(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 200)
	if err != nil {
		t.Fatal(err)
	}
	db.opt.L0CompactionTrig = 2
	mustPut(t, db, 80)
	_ = db.Close()
	hits := 0
	for at := int64(1); at <= 160; at++ {
		fs2 := fs.Clone()
		p := &fsx.CrashPolicy{CrashAt: at}
		fs2.SetPolicy(p)
		db, err := openWith(fs2, 200)
		if err != nil {
			continue
		}
		err = db.CompactAll()
		_ = db.Close()
		if !p.Crashed() && !errors.Is(err, fsx.ErrCrashed) {
			continue
		}
		hits++
		crashed := fs2.Crash()
		db2, err := openWith(crashed, 200)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		checkAll(t, db2, 80, nil)
		_ = db2.Close()
		assertManifestFilesExist(t, crashed)
	}
	if hits == 0 {
		t.Fatal("no crash points during compaction")
	}
}

// c8: finished compaction leaves no tmp or orphan SST; hand-made orphans of
// every kind (.sst/.tmp/.log/MANIFEST/CURRENT.tmp) are collected on reopen.
func TestC8CompactionThenOrphanGC(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 200)
	if err != nil {
		t.Fatal(err)
	}
	db.opt.L0CompactionTrig = 2
	mustPut(t, db, 80)
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	fakes := []string{"/000900.sst", "/000900.sst.tmp", "/000900.log", "/MANIFEST-000900", "/CURRENT.tmp"}
	for _, name := range fakes {
		f, err := fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.Write([]byte("garbage"))
		_ = f.Close()
	}

	db, err = openWith(fs, 200)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checkAll(t, db, 80, nil)
	live := db.LiveFiles()
	ents, _ := db.DirEntries()
	seen := map[string]bool{}
	for _, name := range ents {
		seen[name] = true
		var n uint64
		if _, err := fmt.Sscanf(name, "%d.sst", &n); err == nil && !live[n] {
			t.Fatalf("orphan sst %s", name)
		}
		if strings.HasSuffix(name, ".tmp") {
			t.Fatalf("tmp leftover %s", name)
		}
	}
	for _, fake := range fakes {
		if seen[strings.TrimPrefix(fake, "/")] {
			t.Fatalf("orphan %s survived GC", fake)
		}
	}
	if got := nonLiveLogs(fs); len(got) > 0 {
		t.Fatalf("non-live logs survived GC: %v", got)
	}
}

// c9: CURRENT torn -> the engine must fail fast (DESIGN 8.2 c8 / 317: no
// guessing at some other manifest), never silently open a different version.
func TestC9CURRENTTear(t *testing.T) {
	fs := fsx.NewMemFS()
	p := &fsx.CrashPolicy{TearPath: "/CURRENT", TearBytes: 3}
	fs.SetPolicy(p)
	db, err := openWith(fs, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, 6)
	_ = db.Close()
	crashed := fs.Crash()
	db, err = openWith(crashed, 1<<20)
	if db != nil {
		_ = db.Close()
	}
	if err == nil {
		t.Fatal("torn CURRENT must fail fast, but Open succeeded")
	}
	if !errors.Is(err, version.ErrCorruptManifest) {
		t.Fatalf("want ErrCorruptManifest, got %v", err)
	}
}

func TestC10DoubleRecovery(t *testing.T) {
	TestDoubleRecovery(t)
}

func TestCrashScan2000(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	const nWrites = 600
	points := 0
	lost := 0
	for crashAt := int64(1); crashAt <= 2400; crashAt++ {
		fs := fsx.NewMemFS()
		p := &fsx.CrashPolicy{CrashAt: crashAt}
		fs.SetPolicy(p)
		db, err := openWith(fs, 512)
		if err != nil {
			// Crash during the first Open (before any write was acked): no
			// durability claim to check.
			continue
		}
		acked := map[string]string{}
		for i := 0; i < nWrites; i++ {
			k := fmt.Sprintf("k%04d", i)
			v := fmt.Sprintf("v%04d", i)
			if e := db.Put([]byte(k), []byte(v)); e != nil {
				break
			}
			acked[k] = v
			if i%80 == 79 && !p.Crashed() {
				_ = db.Flush()
			}
		}
		_ = db.Close()
		if !p.Crashed() {
			continue
		}
		points++
		crashed := fs.Crash()
		db2, err := openWith(crashed, 180)
		if err != nil {
			lost++
			t.Logf("reopen err crashAt=%d: %v", crashAt, err)
			continue
		}
		fail := false
		for k, v := range acked {
			got, e := db2.Get([]byte(k))
			if e != nil || string(got) != v {
				fail = true
				t.Logf("data miss crashAt=%d key=%s want=%s got=%q err=%v", crashAt, k, v, got, e)
				break
			}
		}
		_ = db2.Close()
		if fail {
			lost++
		}
	}
	if points < 2000 {
		t.Fatalf("crash points %d want >= 2000", points)
	}
	if lost > 0 {
		t.Fatalf("lost acked writes at %d/%d points", lost, points)
	}
}

func TestSnapshotVsCompaction(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_ = db.Put([]byte("k"), []byte("old"))
	snap := db.Snapshot()
	defer snap.Release()
	_ = db.Put([]byte("k"), []byte("new"))
	_ = db.Delete([]byte("k"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	v, err := snap.Get([]byte("k"))
	if err != nil || string(v) != "old" {
		t.Fatalf("snapshot lost across compaction: %q %v", v, err)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatal("delete not visible")
	}
}

func TestBitFlipDetected(t *testing.T) {
	TestI7BitFlipAcrossFiles(t)
}
