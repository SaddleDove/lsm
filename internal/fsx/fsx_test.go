package fsx

import (
	"os"
	"testing"
)

func TestMemFSCrashDropsDirty(t *testing.T) {
	fs := NewMemFS()
	f, err := fs.Create("/a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("WORLD")); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	c := fs.Crash()
	g, err := c.Open("/a")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 20)
	n, _ := g.Read(buf)
	if string(buf[:n]) != "hello" {
		t.Fatalf("got %q", buf[:n])
	}
}

func TestMemFSRenameRollback(t *testing.T) {
	fs := NewMemFS()
	f, _ := fs.Create("/old")
	_, _ = f.Write([]byte("x"))
	_ = f.Sync()
	_ = f.Close()
	if err := fs.Rename("/old", "/new"); err != nil {
		t.Fatal(err)
	}
	c := fs.Crash()
	if _, err := c.Stat("/new"); !isNotExist(err) {
		t.Fatalf("rename should roll back, err=%v", err)
	}
	if _, err := c.Stat("/old"); err != nil {
		t.Fatalf("old missing: %v", err)
	}
}

func TestMemFSRenameDurableAfterDirsync(t *testing.T) {
	fs := NewMemFS()
	f, _ := fs.Create("/old")
	_, _ = f.Write([]byte("x"))
	_ = f.Sync()
	_ = f.Close()
	_ = fs.Rename("/old", "/new")
	_ = fs.SyncDir("/")
	c := fs.Crash()
	if _, err := c.Stat("/new"); err != nil {
		t.Fatal(err)
	}
}

func TestCrashInjectorStopsWrites(t *testing.T) {
	fs := NewMemFS()
	p := &CrashPolicy{CrashAt: 2}
	fs.SetPolicy(p)
	f, err := fs.Create("/a")
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write([]byte("ab"))
	if err != ErrCrashed {
		t.Fatalf("want crashed got %v", err)
	}
}

func TestStatsCountWrites(t *testing.T) {
	fs := NewMemFS()
	f, _ := fs.Create("/a")
	_, _ = f.Write([]byte("abcd"))
	_ = f.Sync()
	st := fs.Stats().Snapshot()
	if st["bytes_written"] != 4 {
		t.Fatalf("%v", st)
	}
}

func isNotExist(err error) bool {
	return err == os.ErrNotExist
}
