package engine

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"lsm/internal/fsx"
)

func seqFill(t *testing.T, db *DB, n int, vlen int) {
	t.Helper()
	val := make([]byte, vlen)
	for i := range val {
		val[i] = 'x'
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		if err := db.Put(k, val); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWASequentialFill(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{
		Dir: "/", FS: fs, MemtableBytes: 256 << 10,
		Sync: SyncNone, L0CompactionTrig: 4, MaxLevel: 7, LevelMultiplier: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	n := 8000
	vlen := 128
	seqFill(t, db, n, vlen)
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	wa := db.WriteAmp()
	logical := float64(n * (8 + vlen))
	st := db.Stats()
	t.Logf("WA_seq=%.3f bytes_in=%d written=%d logical=%.0f", wa, st["bytes_in"], st["bytes_written"], logical)
	if wa > 8 {
		t.Fatalf("sequential WA %.3f > 8", wa)
	}
	_ = db.Close()
}

func TestWARandomOverwrite(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{
		Dir: "/", FS: fs, MemtableBytes: 256 << 10,
		Sync: SyncNone, L0CompactionTrig: 4, MaxLevel: 7, LevelMultiplier: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	n := 4000
	vlen := 128
	val := make([]byte, vlen)
	for i := range val {
		val[i] = 'y'
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		if err := db.Put(k, val); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%08d", rng.Intn(n)))
		if err := db.Put(k, val); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	wa := db.WriteAmp()
	st := db.Stats()
	deepest := db.DeepestLevel()
	levels := deepest
	if levels < 1 {
		levels = 1
	}
	model := 1.0 + 1.0 + 10.0*float64(levels-1)
	dev := (wa - model) / model
	t.Logf("WA_rand=%.3f bytes_in=%d written=%d deepest=%d model=%.1f dev=%+.1f%%", wa, st["bytes_in"], st["bytes_written"], deepest, model, 100*dev)
	// Absolute bound is gated; the analytic model is reported (asymptotic, see
	// README / DECISIONS) but not asserted at this small scale.
	if wa > 25 {
		t.Fatalf("random WA %.3f > 25", wa)
	}
	_ = db.Close()
}

func TestSpaceAmplification(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{
		Dir: "/", FS: fs, MemtableBytes: 128 << 10,
		Sync: SyncNone, L0CompactionTrig: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	n := 3000
	vlen := 64
	seqFill(t, db, n, vlen)
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	sa := db.SpaceAmp()
	st := db.Stats()
	t.Logf("SA=%.3f live=%d in=%d", sa, st["live_bytes"], st["bytes_in"])
	if sa > 1.5 {
		t.Fatalf("space amp %.3f > 1.5", sa)
	}
	_ = db.Close()
}

func TestReadAmplification(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{
		Dir: "/", FS: fs, MemtableBytes: 64 << 10,
		Sync: SyncNone, L0CompactionTrig: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	n := 2000
	seqFill(t, db, n, 32)
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	db.sstReads.Store(0)
	gets := 200
	for i := 0; i < gets; i++ {
		k := []byte(fmt.Sprintf("k%08d", i*3%n))
		if _, err := db.Get(k); err != nil {
			t.Fatal(err)
		}
	}
	avg := float64(db.sstReads.Load()) / float64(gets)
	t.Logf("read amp avg sst reads/get = %.3f", avg)
	if avg > 3.0 {
		t.Fatalf("read amp %.3f > 3", avg)
	}
	_ = db.Close()
}

func TestRecoveryTime(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{
		Dir: "/", FS: fs, MemtableBytes: 8 << 20,
		Sync: SyncNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	n := 20000
	seqFill(t, db, n, 64)
	_ = db.Close()
	start := time.Now()
	db, err = Open(Options{Dir: "/", FS: fs, MemtableBytes: 8 << 20, Sync: SyncNone})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	t.Logf("recovery %s for %d keys", elapsed, n)
	if elapsed > 2*time.Second {
		t.Fatalf("recovery %s > 2s", elapsed)
	}
}

// TestRecoveryTimeFull is the LSM_FULL-only recovery benchmark at the scale
// claimed by DESIGN 2.2/69 (2s per 100 MB WAL). It is skipped unless LSM_FULL=1
// so the default gates stay fast and small.
func TestRecoveryTimeFull(t *testing.T) {
	if os.Getenv("LSM_FULL") != "1" {
		t.Skip("set LSM_FULL=1 to run the full-scale recovery benchmark")
	}
	mb := 100
	if v := os.Getenv("LSM_RECOVERY_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			mb = n
		}
	}
	const vlen = 100
	// WAL bytes per record: 8 header + 8 seq + 1 kind + 4 klen + 9 key + 4 vlen + val.
	perRec := 34 + vlen
	n := mb * 1024 * 1024 / perRec

	fs := fsx.NewMemFS()
	mem := int64(mb+64) << 20 // large enough that nothing flushes mid-fill
	db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: mem, Sync: SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	seqFill(t, db, n, vlen)
	fill := time.Since(start)
	walBytes := fs.Stats().BytesWritten.Load()
	_ = db.Close()

	recStart := time.Now()
	db, err = Open(Options{Dir: "/", FS: fs, MemtableBytes: mem, Sync: SyncNone})
	rec := time.Since(recStart)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Spot-check recovery actually rebuilt the data.
	if _, err := db.Get([]byte(fmt.Sprintf("k%08d", n/2))); err != nil {
		t.Fatalf("post-recovery get: %v", err)
	}
	t.Logf("recovery %s for %d keys (~%d MB written, target %d MB WAL); fill took %s", rec, n, walBytes>>20, mb, fill)
	if rec > 2*time.Second {
		t.Fatalf("recovery %s > 2s at ~%d MB WAL", rec, walBytes>>20)
	}
}
