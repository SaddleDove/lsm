package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"lsm/internal/engine"
	"lsm/internal/fsx"
)

func main() {
	mode := flag.String("mode", "seq", "seq|rand")
	n := flag.Int("n", 8000, "number of keys")
	vlen := flag.Int("vlen", 128, "value length")
	mem := flag.Int("mem", 256<<10, "memtable bytes")
	dir := flag.String("dir", "", "if set, use OSFS at dir; else MemFS")
	compact := flag.Bool("compact", false, "compact all at end")
	seed := flag.Int64("seed", 1, "rng seed")
	flag.Parse()

	var fs fsx.FS
	work := *dir
	if work == "" {
		fs = fsx.NewMemFS()
		work = "/"
	}
	db, err := engine.Open(engine.Options{
		Dir:              work,
		FS:               fs,
		MemtableBytes:    int64(*mem),
		Sync:             engine.SyncNone,
		L0CompactionTrig: 4,
		MaxLevel:         7,
		LevelMultiplier:  10,
	})
	if err != nil {
		fatal(err)
	}
	val := make([]byte, *vlen)
	for i := range val {
		val[i] = 'x'
	}
	start := time.Now()
	switch *mode {
	case "seq":
		for i := 0; i < *n; i++ {
			k := []byte(fmt.Sprintf("k%08d", i))
			if err := db.Put(k, val); err != nil {
				fatal(err)
			}
		}
	case "rand":
		rng := rand.New(rand.NewSource(*seed))
		for i := 0; i < *n; i++ {
			k := []byte(fmt.Sprintf("k%08d", i))
			if err := db.Put(k, val); err != nil {
				fatal(err)
			}
		}
		for i := 0; i < *n; i++ {
			k := []byte(fmt.Sprintf("k%08d", rng.Intn(*n)))
			if err := db.Put(k, val); err != nil {
				fatal(err)
			}
		}
	default:
		fatal(fmt.Errorf("unknown mode %s", *mode))
	}
	if err := db.Flush(); err != nil {
		fatal(err)
	}
	if *compact {
		if err := db.CompactAll(); err != nil {
			fatal(err)
		}
	}
	elapsed := time.Since(start)
	st := db.Stats()
	wa := db.WriteAmp()
	sa := db.SpaceAmp()
	deepest := db.DeepestLevel()
	_ = db.Close()
	// Analytic model (DESIGN 7.2): WA ~ 1(WAL) + 1(flush) + T*(levels-1),
	// where `levels` is the deepest level the data actually reached. This is
	// measured, not a per-mode constant.
	levels := deepest
	if levels < 1 {
		levels = 1
	}
	model := 1.0 + 1.0 + float64(10)*float64(levels-1)
	fmt.Printf("mode=%s n=%d vlen=%d elapsed=%s\n", *mode, *n, *vlen, elapsed)
	fmt.Printf("bytes_in=%d bytes_written=%d live_bytes=%d deepest_level=%d\n", st["bytes_in"], st["bytes_written"], st["live_bytes"], deepest)
	fmt.Printf("WA=%.4f SA=%.4f model=%.1f\n", wa, sa, model)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
