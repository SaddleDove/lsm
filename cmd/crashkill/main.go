package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"lsm/internal/engine"
)

func main() {
	child := flag.Bool("child", false, "child writer process")
	dir := flag.String("dir", "", "db dir")
	ack := flag.String("ack", "", "ack file")
	n := flag.Int("n", 2000, "keys to write in child")
	trials := flag.Int("trials", 20, "kill -9 trials (parent)")
	seed := flag.Int64("seed", 1, "seed")
	flag.Parse()
	if *child {
		if err := runChild(*dir, *ack, *n); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if *dir == "" {
		tmp, err := os.MkdirTemp("", "lsm-kill-*")
		if err != nil {
			fatal(err)
		}
		*dir = tmp
	}
	rng := rand.New(rand.NewSource(*seed))
	lost := 0
	ok := 0
	midwrite := 0
	self, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	for i := 0; i < *trials; i++ {
		work := filepath.Join(*dir, fmt.Sprintf("t%04d", i))
		_ = os.MkdirAll(work, 0755)
		ackFile := filepath.Join(work, "ACK")
		cmd := exec.Command(self, "-child", "-dir", work, "-ack", ackFile, "-n", strconv.Itoa(*n))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fatal(err)
		}
		// Wait until the child has acknowledged at least one write so the kill
		// lands during writing, not while the DB is still being created.
		ackDeadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(ackDeadline) {
			if fi, err := os.Stat(ackFile); err == nil && fi.Size() > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		wait := time.Duration(5+rng.Intn(40)) * time.Millisecond
		time.Sleep(wait)
		_ = cmd.Process.Kill()
		_ = cmd.Process.Signal(syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
		acked := readAcks(ackFile)
		db, err := engine.Open(engine.Options{
			Dir:           work,
			MemtableBytes: 64 << 10,
			Sync:          engine.SyncEveryWrite,
		})
		if err != nil {
			fmt.Printf("trial %d reopen err: %v acked=%d\n", i, err, len(acked))
			lost++
			continue
		}
		fail := false
		for _, id := range acked {
			k := []byte(fmt.Sprintf("k%08d", id))
			v, err := db.Get(k)
			want := fmt.Sprintf("v%08d", id)
			if err != nil || string(v) != want {
				fail = true
				break
			}
		}
		_ = db.Close()
		if len(acked) > 0 && len(acked) < *n {
			// The kill landed while the child was still writing (the interesting
			// case for a crash-consistency claim).
			midwrite++
		}
		if fail {
			lost++
			fmt.Printf("trial %d LOST acked=%d\n", i, len(acked))
		} else {
			ok++
		}
	}
	fmt.Printf("kill9 trials=%d keys=%d ok=%d lost=%d midwrite=%d\n", *trials, *n, ok, lost, midwrite)
	if *trials > 0 && midwrite == 0 {
		fmt.Printf("warning: no trial was killed mid-write (all children finished or wrote nothing first)\n")
	}
	if lost > 0 {
		os.Exit(1)
	}
}

func runChild(dir, ack string, n int) error {
	db, err := engine.Open(engine.Options{
		Dir:           dir,
		MemtableBytes: 64 << 10,
		Sync:          engine.SyncEveryWrite,
	})
	if err != nil {
		return err
	}
	defer db.Close()
	af, err := os.OpenFile(ack, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer af.Close()
	buf := make([]byte, 4)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k%08d", i))
		v := []byte(fmt.Sprintf("v%08d", i))
		if err := db.Put(k, v); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(buf, uint32(i))
		if _, err := af.Write(buf); err != nil {
			return err
		}
		if err := af.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func readAcks(path string) []int {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []int
	r := bufio.NewReader(f)
	buf := make([]byte, 4)
	for {
		_, err := r.Read(buf)
		if err != nil {
			break
		}
		out = append(out, int(binary.LittleEndian.Uint32(buf)))
	}
	return out
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
