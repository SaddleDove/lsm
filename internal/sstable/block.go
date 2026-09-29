package sstable

import (
	"encoding/binary"
	"errors"

	"lsm/internal/crc32c"
	"lsm/internal/ikey"
)

const (
	restartInterval = 16
	blockTrailer    = 5
)

var ErrCorruptBlock = errors.New("sstable: corrupt block")

func putUvarint(dst []byte, x uint64) []byte {
	for x >= 0x80 {
		dst = append(dst, byte(x)|0x80)
		x >>= 7
	}
	return append(dst, byte(x))
}

func uvarint(b []byte) (uint64, int) {
	var x uint64
	var s uint
	for i := 0; i < len(b); i++ {
		if b[i] < 0x80 {
			if i > 9 || (i == 9 && b[i] > 1) {
				return 0, -1
			}
			return x | uint64(b[i])<<s, i + 1
		}
		x |= uint64(b[i]&0x7f) << s
		s += 7
	}
	return 0, 0
}

func sharedPrefix(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

type blockWriter struct {
	buf      []byte
	restarts []uint32
	count    int
	lastKey  []byte
}

func (w *blockWriter) add(key, val []byte) {
	if w.count%restartInterval == 0 {
		w.restarts = append(w.restarts, uint32(len(w.buf)))
		w.buf = putUvarint(w.buf, 0)
		w.buf = putUvarint(w.buf, uint64(len(key)))
		w.buf = putUvarint(w.buf, uint64(len(val)))
		w.buf = append(w.buf, key...)
		w.buf = append(w.buf, val...)
	} else {
		sh := sharedPrefix(w.lastKey, key)
		w.buf = putUvarint(w.buf, uint64(sh))
		w.buf = putUvarint(w.buf, uint64(len(key)-sh))
		w.buf = putUvarint(w.buf, uint64(len(val)))
		w.buf = append(w.buf, key[sh:]...)
		w.buf = append(w.buf, val...)
	}
	w.lastKey = append(w.lastKey[:0], key...)
	w.count++
}

func (w *blockWriter) finish() []byte {
	if len(w.restarts) == 0 {
		w.restarts = append(w.restarts, 0)
	}
	for _, r := range w.restarts {
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], r)
		w.buf = append(w.buf, tmp[:]...)
	}
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(w.restarts)))
	w.buf = append(w.buf, tmp[:]...)
	data := w.buf
	crc := crc32c.Sum(append(append([]byte{}, data...), 0))
	out := make([]byte, len(data)+blockTrailer)
	copy(out, data)
	out[len(data)] = 0
	binary.LittleEndian.PutUint32(out[len(data)+1:], crc)
	return out
}

func (w *blockWriter) reset() {
	w.buf = w.buf[:0]
	w.restarts = w.restarts[:0]
	w.count = 0
	w.lastKey = w.lastKey[:0]
}

func (w *blockWriter) bytes() int  { return len(w.buf) }
func (w *blockWriter) empty() bool { return w.count == 0 }

type blockHandle struct {
	Off  uint64
	Size uint64
}

func encodeHandle(h blockHandle) []byte {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], h.Off)
	binary.LittleEndian.PutUint64(b[8:16], h.Size)
	return b[:]
}

func decodeHandle(b []byte) (blockHandle, error) {
	if len(b) < 16 {
		return blockHandle{}, ErrCorruptBlock
	}
	return blockHandle{
		Off:  binary.LittleEndian.Uint64(b[0:8]),
		Size: binary.LittleEndian.Uint64(b[8:16]),
	}, nil
}

type blockReader struct {
	data     []byte
	restarts []uint32
}

func parseBlock(raw []byte) (*blockReader, error) {
	if len(raw) < blockTrailer+4 {
		return nil, ErrCorruptBlock
	}
	data := raw[:len(raw)-blockTrailer]
	typ := raw[len(raw)-blockTrailer]
	want := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	got := crc32c.Sum(append(append([]byte{}, data...), typ))
	if got != want {
		return nil, ErrCorruptBlock
	}
	if len(data) < 4 {
		return nil, ErrCorruptBlock
	}
	nre := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	if nre < 1 || 4*nre+4 > len(data) {
		return nil, ErrCorruptBlock
	}
	restOff := len(data) - 4 - 4*nre
	restarts := make([]uint32, nre)
	for i := 0; i < nre; i++ {
		restarts[i] = binary.LittleEndian.Uint32(data[restOff+4*i:])
	}
	return &blockReader{data: data[:restOff], restarts: restarts}, nil
}

type blockIter struct {
	br      *blockReader
	off     int
	key     []byte
	val     []byte
	restart int
	valid   bool
	err     error
}

func (br *blockReader) iter() *blockIter {
	return &blockIter{br: br}
}

func (it *blockIter) readEntry(shared int) bool {
	if it.off >= len(it.br.data) {
		it.valid = false
		return false
	}
	sh, n := uvarint(it.br.data[it.off:])
	if n <= 0 {
		it.err = ErrCorruptBlock
		it.valid = false
		return false
	}
	it.off += n
	un, n := uvarint(it.br.data[it.off:])
	if n <= 0 {
		it.err = ErrCorruptBlock
		it.valid = false
		return false
	}
	it.off += n
	vl, n := uvarint(it.br.data[it.off:])
	if n <= 0 {
		it.err = ErrCorruptBlock
		it.valid = false
		return false
	}
	it.off += n
	if shared < 0 {
		shared = int(sh)
	}
	if it.off+int(un)+int(vl) > len(it.br.data) {
		it.err = ErrCorruptBlock
		it.valid = false
		return false
	}
	if shared > len(it.key) {
		it.err = ErrCorruptBlock
		it.valid = false
		return false
	}
	nk := make([]byte, shared+int(un))
	copy(nk, it.key[:shared])
	copy(nk[shared:], it.br.data[it.off:it.off+int(un)])
	it.off += int(un)
	it.val = it.br.data[it.off : it.off+int(vl)]
	it.off += int(vl)
	it.key = nk
	it.valid = true
	return true
}

func (it *blockIter) SeekToFirst() {
	it.off = 0
	it.key = nil
	it.restart = 0
	it.readEntry(0)
}

func (it *blockIter) Next() {
	if !it.valid {
		return
	}
	it.readEntry(-1)
}

func (it *blockIter) Seek(target []byte) {
	lo, hi := 0, len(it.br.restarts)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		it.off = int(it.br.restarts[mid])
		it.key = nil
		if !it.readEntry(0) {
			return
		}
		if ikey.Compare(it.key, target) <= 0 {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	it.off = int(it.br.restarts[lo])
	it.key = nil
	it.restart = lo
	if !it.readEntry(0) {
		return
	}
	for it.valid && ikey.Compare(it.key, target) < 0 {
		it.readEntry(-1)
	}
}

func (it *blockIter) Valid() bool   { return it.valid }
func (it *blockIter) Key() []byte   { return it.key }
func (it *blockIter) Value() []byte { return it.val }
func (it *blockIter) Err() error    { return it.err }
