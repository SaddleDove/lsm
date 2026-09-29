package crc32c

import "testing"

func TestKnownVectors(t *testing.T) {
	// Castagnoli check vector: CRC32C("123456789") = 0xE3069283.
	if got := Sum([]byte("123456789")); got != 0xE3069283 {
		t.Fatalf("crc32c(\"123456789\") = %#x, want 0xE3069283", got)
	}
	if got := Sum(nil); got != 0 {
		t.Fatalf("crc32c(nil) = %#x, want 0", got)
	}
}

func TestSingleBitChangesChecksum(t *testing.T) {
	base := []byte("the quick brown fox jumps over the lazy dog")
	want := Sum(base)
	flipped := append([]byte(nil), base...)
	flipped[7] ^= 0x01
	if got := Sum(flipped); got == want {
		t.Fatal("checksum unchanged after a single-bit flip")
	}
}

func TestEmptyVsOneZeroByte(t *testing.T) {
	if Sum(nil) != 0 {
		t.Fatalf("crc32c(nil) = %#x, want 0", Sum(nil))
	}
	if Sum([]byte{0}) == 0 {
		t.Fatal("crc32c([0]) unexpectedly zero")
	}
}
