package engine

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"lsm/internal/fsx"
)

// collectScan drains an Iterator into ordered key/value pairs.
func collectScan(t *testing.T, it *Iterator, err error) ([]string, []string) {
	t.Helper()
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	var ks, vs []string
	for it.Valid() {
		ks = append(ks, string(it.Key()))
		vs = append(vs, string(it.Value()))
		it.Next()
	}
	if err := it.Err(); err != nil {
		t.Fatalf("scan iter: %v", err)
	}
	return ks, vs
}

func refScan(ref map[string]string, start, end []byte) ([]string, []string) {
	var keys []string
	for k := range ref {
		if start != nil && k < string(start) {
			continue
		}
		if end != nil && k >= string(end) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var vs []string
	for _, k := range keys {
		vs = append(vs, ref[k])
	}
	return keys, vs
}

func assertScanEq(t *testing.T, db *DB, ref map[string]string, start, end []byte) {
	t.Helper()
	wantK, wantV := refScan(ref, start, end)
	it, err := db.Scan(start, end)
	gotK, gotV := collectScan(t, it, err)
	if len(gotK) != len(wantK) {
		t.Fatalf("scan len %d want %d\n got=%v\nwant=%v", len(gotK), len(wantK), gotK, wantK)
	}
	for i := range wantK {
		if gotK[i] != wantK[i] || gotV[i] != wantV[i] {
			t.Fatalf("scan[%d] = (%q,%q) want (%q,%q)", i, gotK[i], gotV[i], wantK[i], wantV[i])
		}
	}
}

// TestScanMatchesReference drives a deterministic random op sequence (puts and
// deletes, with flushes and compactions mixed in) against an ordered reference
// model and verifies full-range and sub-range scans after every phase.
func TestScanMatchesReference(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 2048, Sync: SyncNone, L0CompactionTrig: 3, MaxLevel: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ref := map[string]string{}
	rng := rand.New(rand.NewSource(99))
	const nKeys = 400
	for i := 0; i < 1200; i++ {
		k := fmt.Sprintf("k%05d", rng.Intn(nKeys))
		switch rng.Intn(10) {
		case 0, 1:
			if err := db.Delete([]byte(k)); err != nil {
				t.Fatal(err)
			}
			delete(ref, k)
		default:
			v := fmt.Sprintf("v%05d-%05d", i, rng.Intn(1000))
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			ref[k] = v
		}
		if i%150 == 149 {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if i%300 == 299 {
			if err := db.CompactAll(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	assertScanEq(t, db, ref, nil, nil)
	assertScanEq(t, db, ref, []byte("k00050"), []byte("k00150"))
	assertScanEq(t, db, ref, []byte("k00100"), nil)
	assertScanEq(t, db, ref, nil, []byte("k00010"))
	assertScanEq(t, db, ref, []byte("k99999"), nil) // empty range
}

// TestScanAcrossFlushAndCompaction forces several memtable flushes and a full
// compaction, then checks scan sees exactly the latest versions.
func TestScanAcrossFlushAndCompaction(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 256, Sync: SyncNone, L0CompactionTrig: 2, MaxLevel: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ref := map[string]string{}
	for round := 0; round < 6; round++ {
		for i := 0; i < 50; i++ {
			k := fmt.Sprintf("k%04d", i)
			v := fmt.Sprintf("r%d-%d", round, i)
			if err := db.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			ref[k] = v
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 50; i += 2 {
		k := fmt.Sprintf("k%04d", i)
		if err := db.Delete([]byte(k)); err != nil {
			t.Fatal(err)
		}
		delete(ref, k)
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	assertScanEq(t, db, ref, nil, nil)
	// A reopen (recovery from manifest + WAL) must scan identically.
	_ = db.Close()
	db2, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 256, Sync: SyncNone, L0CompactionTrig: 2, MaxLevel: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	assertScanEq(t, db2, ref, nil, nil)
}

// TestScanSnapshot verifies Snapshot.Scan is isolated from later writes/deletes
// and from compaction.
func TestScanSnapshot(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 4096, Sync: SyncNone, L0CompactionTrig: 3, MaxLevel: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 30; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("old"))
	}
	snap := db.Snapshot()
	defer snap.Release()
	for i := 0; i < 30; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("new"))
	}
	_ = db.Delete([]byte("k000"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	// Snapshot sees all 30 "old" values.
	sit, serr := snap.Scan(nil, nil)
	ks, vs := collectScan(t, sit, serr)
	if len(ks) != 30 {
		t.Fatalf("snapshot scan len=%d want 30", len(ks))
	}
	for i, v := range vs {
		if v != "old" {
			t.Fatalf("snapshot value[%d]=%q want old", i, v)
		}
	}
	// Current view: k000 deleted, the rest "new".
	ref := map[string]string{}
	for i := 1; i < 30; i++ {
		ref[fmt.Sprintf("k%03d", i)] = "new"
	}
	assertScanEq(t, db, ref, nil, nil)
}

// TestScanTombstoneSuppression ensures a deleted key that only has a tombstone
// (after compaction dropped the older value) is not emitted.
func TestScanTombstoneSuppression(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 128, Sync: SyncNone, L0CompactionTrig: 2, MaxLevel: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 20; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v"))
	}
	_ = db.Delete([]byte("k005"))
	_ = db.Flush()
	_ = db.CompactAll()
	it, ierr := db.Scan(nil, nil)
	ks, _ := collectScan(t, it, ierr)
	for _, k := range ks {
		if k == "k005" {
			t.Fatal("tombstoned k005 leaked into scan")
		}
	}
	if len(ks) != 19 {
		t.Fatalf("scan len=%d want 19 (%v)", len(ks), ks)
	}
	if _, err := db.Get([]byte("k005")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("k005 should be gone, got %v", err)
	}
	if !bytes.Equal([]byte("k019"), []byte(ks[len(ks)-1])) {
		t.Fatalf("last key %q", ks[len(ks)-1])
	}
}
