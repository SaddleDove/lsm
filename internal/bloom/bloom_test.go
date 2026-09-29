package bloom

import (
	"math"
	"testing"
)

func TestBloomBasic(t *testing.T) {
	var keys [][]byte
	for i := 0; i < 1000; i++ {
		keys = append(keys, []byte{byte(i >> 8), byte(i)})
	}
	f := New(keys, 10)
	for _, k := range keys {
		if !f.MayContain(k) {
			t.Fatalf("false negative")
		}
	}
}

// TestBloomFPRBound enforces DESIGN 2.2 / 68: the measured false-positive rate
// must be within 1.2x the theoretical rate for bits/key=10, k=7. The keys are
// fixed, so the measurement is deterministic (no statistical flakiness).
func TestBloomFPRBound(t *testing.T) {
	const (
		n        = 10000
		nProbes  = 10000
		bitsPerK = 10.0
		k        = 7.0
	)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte{byte(i >> 8), byte(i)}
	}
	f := New(keys, 10)
	for _, kk := range keys {
		if !f.MayContain(kk) {
			t.Fatal("false negative: a member was reported absent")
		}
	}
	fp := 0
	for i := 0; i < nProbes; i++ {
		x := (1 << 20) + i
		probe := []byte{byte(x >> 16), byte(x >> 8), byte(x)}
		if f.MayContain(probe) {
			fp++
		}
	}
	theory := math.Pow(1-math.Exp(-k/bitsPerK), k)
	expected := theory * nProbes
	limit := math.Ceil(expected * 1.2)
	got := float64(fp)
	t.Logf("FPR=%d/%d=%.4f%% theoretical=%.4f%% (%.1f expected) limit(1.2x)=%.0f", fp, nProbes, 100*got/nProbes, 100*theory, expected, limit)
	if got > limit {
		t.Fatalf("bloom FPR %.4f%% exceeds 1.2x theoretical (%.4f%%): fp=%d limit=%.0f", 100*got/nProbes, 100*1.2*theory, fp, limit)
	}
}
