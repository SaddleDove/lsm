package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"lsm/internal/fsx"
)

// fileSetHash is a stable hash of the sorted set of files in the DB directory,
// used for the "file set must be identical" half of I5 (DESIGN 2.1 / 52).
func fileSetHash(t *testing.T, fs fsx.FS) string {
	t.Helper()
	ents, err := fs.ReadDir("/")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	sum := sha256.Sum256([]byte(strings.Join(names, "\n")))
	return hex.EncodeToString(sum[:])
}

func TestI1DurabilityAckedWrites(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, 20)
	_ = db.Close()
	db, err = openWith(fs.Crash(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checkAll(t, db, 20, nil)
}

func TestI2AtomicBatch(t *testing.T) {
	TestCrashPartialBatch(t)
}

func TestI3NoResurrection(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 128)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Put([]byte("k"), []byte("v"))
	_ = db.Delete([]byte("k"))
	_ = db.Flush()
	_ = db.CompactAll()
	_ = db.Close()
	db, err = openWith(fs, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted key resurrected")
	}
}

// I4 (DESIGN 2.1 / 49): after recovery last_sequence must be >= every acked
// write, and new writes must not reuse a sequence number.
func TestI4SeqNoReuseAfterRecovery(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 256)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		_ = db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v"))
	}
	lastAcked := db.LastSeq()
	_ = db.Flush()
	_ = db.Close()

	db, err = openWith(fs.Crash(), 256)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.LastSeq(); got < lastAcked {
		t.Fatalf("recovered seq %d < acked %d", got, lastAcked)
	}
	base := db.LastSeq()
	_ = db.Put([]byte("fresh"), []byte("v"))
	if got := db.LastSeq(); got <= base {
		t.Fatalf("post-recovery write reused seq: %d -> %d", base, got)
	}
}

func TestI4SeqMonotonic(t *testing.T) {
	TestSeqMonotonic(t)
	TestI4SeqNoReuseAfterRecovery(t)
}

func TestI5RecoveryIdempotent(t *testing.T) {
	TestRecoveryIdempotent(t)
}

func TestI6SnapshotConsistent(t *testing.T) {
	TestSnapshotVsCompaction(t)
}

// I7 (DESIGN 2.1 / 54): sample bit flips across SSTable, MANIFEST and WAL.
func TestI7NoSilentCorruption(t *testing.T) {
	TestI7BitFlipAcrossFiles(t)
}

func TestI8NoFileLeak(t *testing.T) {
	TestNoFileLeak(t)
}

func TestWALThenMemtableOrder(t *testing.T) {
	fs := fsx.NewMemFS()
	p := &fsx.CrashPolicy{CrashAt: 0}
	fs.SetPolicy(p)
	db, err := openWith(fs, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	before := fs.Stats().BytesSynced.Load()
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	after := fs.Stats().BytesSynced.Load()
	if after <= before {
		t.Fatal("put did not fsync WAL before becoming visible")
	}
	v, err := db.Get([]byte("k"))
	if err != nil || string(v) != "v" {
		t.Fatal(err)
	}
	_ = db.Close()
}

func TestReplaySkipOldSeq(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 64)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Put([]byte("k"), []byte("v1"))
	_ = db.Flush()
	seq := db.LastSeq()
	_ = db.Delete([]byte("k"))
	_ = db.Close()
	db, err = openWith(fs, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.LastSeq() < seq {
		t.Fatal("seq went backwards")
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Fatal("delete lost across recovery")
	}
}

func TestGroupCommitAfterFsync(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := Open(Options{Dir: "/", FS: fs, MemtableBytes: 1 << 20, Sync: SyncGroup})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var b WriteBatch
	b.Put([]byte("a"), []byte("1"))
	before := fs.Stats().Fsyncs.Load()
	if err := db.Write(&b, WriteOptions{Sync: true}); err != nil {
		t.Fatal(err)
	}
	if fs.Stats().Fsyncs.Load() <= before {
		t.Fatal("group commit acked without fsync")
	}
}

func TestManyKeysSurviveLevels(t *testing.T) {
	fs := fsx.NewMemFS()
	db, err := openWith(fs, 512)
	if err != nil {
		t.Fatal(err)
	}
	n := 400
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		if err := db.Put(k, bytesRepeat(byte(i), 32)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.CompactAll(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		v, err := db.Get(k)
		if err != nil || len(v) != 32 {
			t.Fatalf("i=%d err=%v", i, err)
		}
	}
	_ = db.Close()
	db, err = openWith(fs, 512)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%05d", i))
		if _, err := db.Get(k); err != nil {
			t.Fatalf("reopen i=%d %v", i, err)
		}
	}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
