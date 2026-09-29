package bloom

const BloomK = 7

type Filter []byte

func hash(data []byte, seed uint32) uint32 {
	h := seed
	for _, c := range data {
		h ^= uint32(c)
		h *= 0x01000193
	}
	// MurmurHash3 fmix32 finalizer: the raw FNV-1a value has weak low bits,
	// which hurt the double-hashing modulo. Finalizing keeps the measured FPR
	// within the DESIGN 2.2 bound of 1.2x the theoretical rate.
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

func New(keys [][]byte, bitsPerKey int) Filter {
	if bitsPerKey < 1 {
		bitsPerKey = 10
	}
	n := len(keys)
	if n == 0 {
		return Filter{0}
	}
	bits := n * bitsPerKey
	if bits < 64 {
		bits = 64
	}
	k := BloomK
	if bitsPerKey != 10 {
		k = int(float64(bitsPerKey)*0.69 + 0.5)
		if k < 1 {
			k = 1
		}
		if k > 30 {
			k = 30
		}
	}
	nbytes := (bits + 7) / 8
	f := make([]byte, nbytes+1)
	f[nbytes] = byte(k)
	nbits := uint32(nbytes * 8)
	for _, key := range keys {
		h := hash(key, 0xbc9f1d34)
		delta := (h >> 17) | (h << 15)
		for i := 0; i < k; i++ {
			bit := h % nbits
			f[bit/8] |= 1 << (bit % 8)
			h += delta
		}
	}
	return Filter(f)
}

func (f Filter) MayContain(key []byte) bool {
	if len(f) < 2 {
		return true
	}
	k := int(f[len(f)-1])
	if k > 30 {
		return true
	}
	nbits := uint32((len(f) - 1) * 8)
	if nbits == 0 {
		return true
	}
	h := hash(key, 0xbc9f1d34)
	delta := (h >> 17) | (h << 15)
	for i := 0; i < k; i++ {
		bit := h % nbits
		if f[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}
