package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Gate struct {
	Name    string `json:"name"`
	Pass    bool   `json:"pass"`
	Detail  string `json:"detail"`
	Elapsed string `json:"elapsed"`
}

type Report struct {
	Generated time.Time      `json:"generated"`
	Gates     []Gate         `json:"gates"`
	Metrics   map[string]any `json:"metrics"`
	Pass      bool           `json:"pass"`
}

// miscTests are substantive tests that no other gate pattern selects.
const miscTests = "TestCrashFlushThenManifest|TestCrashTornWAL|TestCrashDeterministicScan|" +
	"TestCrashWALBeforeAck|TestWALThenMemtableOrder|TestGroupCommitAfterFsync|" +
	"TestManyKeysSurviveLevels|TestReplaySkipOldSeq|TestOverwriteAfterFlush|" +
	"TestCompactionKeepsData|TestBitFlipDetected"

func main() {
	root := flag.String("root", ".", "module root")
	out := flag.String("out", "testdata", "output dir")
	full := flag.Bool("full", false, "also run the 100 MB recovery benchmark")
	flag.Parse()
	_ = os.MkdirAll(*out, 0755)

	jsonPath := filepath.Join(*out, "report.json")
	mdPath := filepath.Join(*root, "PROGRESS.md")
	committedJSON, _ := os.ReadFile(jsonPath)
	committedMD, _ := os.ReadFile(mdPath)

	var gates []Gate
	metrics := map[string]any{}

	gates = append(gates, runGate("G1_unit", func() (bool, string) {
		cmd := exec.Command("go", "test", "-count=1", "./internal/ikey", "./internal/bloom", "./internal/memtable", "./internal/wal", "./internal/sstable", "./internal/fsx", "./internal/version", "./internal/crc32c")
		cmd.Dir = *root
		b, err := cmd.CombinedOutput()
		return err == nil, strings.TrimSpace(string(b))
	}))

	gates = append(gates, runGate("G2_invariants_I1_I8", func() (bool, string) {
		cmd := exec.Command("go", "test", "-count=1", "./internal/engine", "-run",
			"TestI[1-8]|TestPutGet|TestDelete|TestBatch|TestFlush|TestSnapshot|TestSeq|TestRecoveryIdempotent|TestNoFileLeak|TestSilent|TestScan|TestWriteRejectsOversizeBatch")
		cmd.Dir = *root
		b, err := cmd.CombinedOutput()
		return err == nil, tail(string(b), 2000)
	}))

	gates = append(gates, runGate("G3_crash_matrix_c1_c10", func() (bool, string) {
		cmd := exec.Command("go", "test", "-count=1", "./internal/engine", "-run", "TestC[0-9]")
		cmd.Dir = *root
		b, err := cmd.CombinedOutput()
		return err == nil, tail(string(b), 2000)
	}))

	gates = append(gates, runGate("G4_crash_scan_2000", func() (bool, string) {
		cmd := exec.Command("go", "test", "-count=1", "-timeout", "10m", "./internal/engine", "-run", "TestCrashScan2000")
		cmd.Dir = *root
		b, err := cmd.CombinedOutput()
		return err == nil, tail(string(b), 2000)
	}))

	killTrials := "5"
	killN := "80"
	if *full {
		killTrials = "200"
		killN = "400"
	}
	gates = append(gates, runGate("G5_kill9", func() (bool, string) {
		cmd := exec.Command("go", "run", "./cmd/crashkill", "-trials", killTrials, "-n", killN)
		cmd.Dir = *root
		b, err := cmd.CombinedOutput()
		s := string(b)
		metrics["kill9_trials"] = parseFloat(s, "trials=")
		metrics["kill9_keys"] = parseFloat(s, "keys=")
		metrics["kill9_ok"] = parseFloat(s, "ok=")
		metrics["kill9_lost"] = parseFloat(s, "lost=")
		metrics["kill9_midwrite"] = parseFloat(s, "midwrite=")
		return err == nil, tail(strings.TrimSpace(s), 2000)
	}))

	gates = append(gates, runGate("G6_WA_seq", func() (bool, string) {
		cmd := exec.Command("go", "run", "./cmd/kvbench", "-mode", "seq", "-n", "8000", "-vlen", "128")
		cmd.Dir = *root
		b, err := cmd.CombinedOutput()
		s := string(b)
		wa := parseFloat(s, "WA=")
		model := parseFloat(s, "model=")
		metrics["WA_seq"] = wa
		metrics["WA_seq_model"] = model
		metrics["WA_seq_raw"] = strings.TrimSpace(s)
		if model > 0 && wa > 0 {
			metrics["WA_seq_model_dev"] = (wa - model) / model
		}
		ok := err == nil && wa > 0 && wa <= 8
		return ok, fmt.Sprintf("WA=%.4f <= 8 model=%.1f err=%v\n%s", wa, model, err, s)
	}))

	gates = append(gates, runGate("G7_WA_rand", func() (bool, string) {
		cmd := exec.Command("go", "run", "./cmd/kvbench", "-mode", "rand", "-n", "4000", "-vlen", "128")
		cmd.Dir = *root
		b, err := cmd.CombinedOutput()
		s := string(b)
		wa := parseFloat(s, "WA=")
		model := parseFloat(s, "model=")
		metrics["WA_rand"] = wa
		metrics["WA_rand_model"] = model
		metrics["WA_rand_raw"] = strings.TrimSpace(s)
		if model > 0 && wa > 0 {
			metrics["WA_rand_model_dev"] = (wa - model) / model
		}
		// Absolute bound is gated. The analytic model (from the measured deepest
		// level) is reported, not asserted: it is asymptotic and not tight at the
		// default benchmark scale (see README "Write amplification").
		ok := err == nil && wa > 0 && wa <= 25
		return ok, fmt.Sprintf("WA=%.4f <= 25 model=%.1f err=%v\n%s", wa, model, err, s)
	}))

	gates = append(gates, runGate("G8_space_read_amp", func() (bool, string) {
		cmd := exec.Command("go", "test", "-count=1", "-v", "./internal/engine", "-run", "TestSpaceAmplification|TestReadAmplification")
		cmd.Dir = *root
		b, err := cmd.CombinedOutput()
		return err == nil, tail(string(b), 2000)
	}))

	gates = append(gates, runGate("G9_recovery", func() (bool, string) {
		cmd := exec.Command("go", "test", "-count=1", "-v", "./internal/engine", "-run", "TestRecoveryTime")
		cmd.Dir = *root
		b, err := cmd.CombinedOutput()
		return err == nil, tail(string(b), 1500)
	}))

	if *full {
		gates = append(gates, runGate("G9b_recovery_100MB", func() (bool, string) {
			cmd := exec.Command("go", "test", "-count=1", "-v", "./internal/engine", "-run", "TestRecoveryTimeFull")
			cmd.Dir = *root
			cmd.Env = append(os.Environ(), "LSM_FULL=1")
			b, err := cmd.CombinedOutput()
			return err == nil, tail(string(b), 2000)
		}))
	}

	gates = append(gates, runGate("G10_misc_tests", func() (bool, string) {
		cmd := exec.Command("go", "test", "-count=1", "-timeout", "10m", "./internal/engine", "-run", miscTests)
		cmd.Dir = *root
		b, err := cmd.CombinedOutput()
		return err == nil, tail(string(b), 2000)
	}))

	pass := true
	for _, g := range gates {
		if !g.Pass {
			pass = false
		}
	}
	rep := Report{Generated: time.Now().UTC(), Gates: gates, Metrics: metrics, Pass: pass}

	// Evidence reproducibility (DESIGN 7.3 / 9.3): freshly generated evidence
	// must equal the committed evidence modulo timestamps/durations. The
	// reproducibility gate itself is excluded from the comparison (otherwise it
	// could never reach a fixed point).
	jb := marshalReport(rep)
	md := renderProgress(rep)
	okJ := eqJSON(committedJSON, jb)
	okM := eqMD(committedMD, []byte(md))
	detail := fmt.Sprintf("report.json=%v PROGRESS.md=%v", okJ, okM)
	if len(committedJSON) == 0 && len(committedMD) == 0 {
		detail += " (no committed evidence found)"
	}
	g11Pass := okJ && okM
	if *full {
		// -full adds gates and so cannot match the committed default evidence.
		g11Pass = true
		detail = "skipped in -full mode"
	}
	g11 := Gate{Name: "G11_evidence_reproducible", Pass: g11Pass, Detail: detail}
	gates = append(gates, g11)
	rep.Gates = gates
	if !g11.Pass {
		rep.Pass = false
	}
	jb = marshalReport(rep)
	md = renderProgress(rep)

	if err := os.WriteFile(jsonPath, jb, 0644); err != nil {
		fatal(err)
	}
	if err := os.WriteFile(mdPath, []byte(md), 0644); err != nil {
		fatal(err)
	}
	fmt.Printf("wrote %s and %s pass=%v\n", jsonPath, mdPath, rep.Pass)
	if !rep.Pass {
		os.Exit(1)
	}
}

func runGate(name string, fn func() (bool, string)) Gate {
	start := time.Now()
	ok, detail := fn()
	fmt.Printf("gate %s pass=%v elapsed=%s\n", name, ok, time.Since(start).Truncate(time.Millisecond))
	return Gate{Name: name, Pass: ok, Detail: detail, Elapsed: time.Since(start).String()}
}

func renderProgress(r Report) string {
	var b strings.Builder
	b.WriteString("# LSM engine gates\n\n")
	b.WriteString("Generated by `go run ./cmd/report`. Do not edit by hand.\n\n")
	b.WriteString(fmt.Sprintf("generated: %s\n\n", r.Generated.Format(time.RFC3339)))
	if r.Pass {
		b.WriteString("**PASS**\n\n")
	} else {
		b.WriteString("**FAIL**\n\n")
	}
	b.WriteString("| gate | pass | elapsed |\n|---|---|---|\n")
	for _, g := range r.Gates {
		p := "FAIL"
		if g.Pass {
			p = "PASS"
		}
		b.WriteString(fmt.Sprintf("| %s | %s | %s |\n", g.Name, p, g.Elapsed))
	}
	b.WriteString("\n## metrics\n\n")
	if v, ok := r.Metrics["WA_seq"]; ok {
		b.WriteString(fmt.Sprintf("- WA sequential: %v (limit 8)\n", v))
	}
	if v, ok := r.Metrics["WA_rand"]; ok {
		b.WriteString(fmt.Sprintf("- WA random: %v (limit 25)\n", v))
	}
	if v, ok := r.Metrics["WA_rand_model"]; ok {
		b.WriteString(fmt.Sprintf("- WA random model (from deepest level): %v\n", v))
	}
	if v, ok := r.Metrics["WA_rand_model_dev"]; ok {
		b.WriteString(fmt.Sprintf("- WA random deviation from model: %v (reported, not gated)\n", v))
	}
	if v, ok := r.Metrics["kill9_trials"]; ok {
		b.WriteString(fmt.Sprintf("- kill -9 trials: %v x %v keys (midwrite=%v, lost=%v)\n",
			v, r.Metrics["kill9_keys"], r.Metrics["kill9_midwrite"], r.Metrics["kill9_lost"]))
	}
	b.WriteString("\n## thresholds\n\n")
	b.WriteString("- I1-I8 binary pass (incl. bit-flip sampling across SST/MANIFEST/WAL)\n")
	b.WriteString("- WA seq <= 8; WA rand <= 25\n")
	b.WriteString("- space amp <= 1.5; read amp <= 3 sst/Get\n")
	b.WriteString("- recovery <= 2s: small default gate; ~100MB WAL in `LSM_FULL=1` mode\n")
	b.WriteString("- WA analytic model reported (asymptotic, not gated) \n")
	return b.String()
}

func marshalReport(r Report) []byte {
	b, _ := json.MarshalIndent(r, "", "  ")
	return b
}

// eqJSON compares two report.json blobs after dropping the reproducibility gate
// and erasing timestamps/durations/elapsed fields.
func eqJSON(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	na, nb := normalize(normJSON(a)), normalize(normJSON(b))
	if na != nb && os.Getenv("LSM_REPRO_DEBUG") != "" {
		i := 0
		for i < len(na) && i < len(nb) && na[i] == nb[i] {
			i++
		}
		lo := i - 80
		if lo < 0 {
			lo = 0
		}
		fmt.Fprintf(os.Stderr, "repro diff at %d\n committed: %q\n new:       %q\n", i, clip(na, lo, i+120), clip(nb, lo, i+120))
	}
	return na == nb
}

func clip(s string, lo, hi int) string {
	if hi > len(s) {
		hi = len(s)
	}
	if lo > hi {
		return ""
	}
	return s[lo:hi]
}

func normJSON(b []byte) string {
	var r Report
	if err := json.Unmarshal(b, &r); err != nil {
		return string(b)
	}
	r.Generated = time.Time{}
	r.Pass = false
	var keep []Gate
	for _, g := range r.Gates {
		if g.Name == "G11_evidence_reproducible" {
			continue
		}
		g.Elapsed = ""
		keep = append(keep, g)
	}
	r.Gates = keep
	out, _ := json.MarshalIndent(r, "", "  ")
	return string(out)
}

// eqMD compares two PROGRESS.md blobs after dropping the reproducibility row and
// erasing timestamps/durations.
func eqMD(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	return normalize(normMD(string(a))) == normalize(normMD(string(b)))
}

func normMD(s string) string {
	var keep []string
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, "G11_evidence_reproducible") {
			continue
		}
		if line == "**PASS**" || line == "**FAIL**" {
			line = "**RESULT**" // overall pass depends on G11, which we exclude
		}
		keep = append(keep, line)
	}
	return strings.Join(keep, "\n")
}

func parseFloat(s, key string) float64 {
	i := strings.Index(s, key)
	if i < 0 {
		return 0
	}
	rest := s[i+len(key):]
	fields := strings.FieldsFunc(rest, func(r rune) bool {
		return r == ' ' || r == '\n' || r == '\t'
	})
	if len(fields) == 0 {
		return 0
	}
	f, _ := strconv.ParseFloat(fields[0], 64)
	return f
}

var (
	tsRE  = regexp.MustCompile(`[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z?`)
	durRE = regexp.MustCompile(`[0-9]+(\.[0-9]+)?(ns|µs|us|ms|s)`)
)

func normalize(s string) string {
	s = tsRE.ReplaceAllString(s, "<ts>")
	s = durRE.ReplaceAllString(s, "<dur>")
	return s
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
