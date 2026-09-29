package ikey

import (
	"bytes"
	"testing"
)

func TestEncodeRoundtrip(t *testing.T) {
	k := Encode([]byte("abc"), 42, KindDelete)
	if !bytes.Equal(UserKey(k), []byte("abc")) {
		t.Fatalf("user %q", UserKey(k))
	}
	if Seq(k) != 42 || KindOf(k) != KindDelete {
		t.Fatalf("seq=%d kind=%d", Seq(k), KindOf(k))
	}
}

func TestOrderSeqDescending(t *testing.T) {
	a := Encode([]byte("k"), 10, KindValue)
	b := Encode([]byte("k"), 9, KindValue)
	if Compare(a, b) >= 0 {
		t.Fatalf("higher seq should sort first")
	}
	if Compare(Encode([]byte("a"), 1, KindValue), Encode([]byte("b"), 99, KindValue)) >= 0 {
		t.Fatalf("user key order")
	}
}
