package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"lsm/internal/bloom"
	"lsm/internal/crc32c"
	"lsm/internal/fsx"
	"lsm/internal/ikey"
)

const (
	FooterSize    = 48
	Magic         = 0x88e241b785f4cff7
	FormatVersion = 1
	DefaultBlock  = 4096
	BitsPerKey    = 10
	BloomK        = 7
)

var ErrCorruptTable = errors.New("sstable: corrupt table")

type Writer struct {
	fs       fsx.FS
	dir      string
	num      uint64
	tmp      string
	final    string
	f        fsx.File
	off      uint64
	data     blockWriter
	index    blockWriter
	keys     [][]byte
	smallest []byte
	largest  []byte
	nkv      int
	blockSz  int
}

func Filename(num uint64) string {
	return fmt.Sprintf("%06d.sst", num)
}

func TmpName(num uint64) string {
	return Filename(num) + ".tmp"
}

func NewWriter(fs fsx.FS, dir string, num uint64, blockSize int) (*Writer, error) {
	if blockSize <= 0 {
		blockSize = DefaultBlock
	}
	tmp := path.Join(dir, TmpName(num))
	f, err := fs.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &Writer{
		fs:      fs,
		dir:     dir,
		num:     num,
		tmp:     tmp,
		final:   path.Join(dir, Filename(num)),
		f:       f,
		blockSz: blockSize,
	}, nil
}

func (w *Writer) Add(key, val []byte) error {
	if w.data.bytes() >= w.blockSz && !w.data.empty() {
		if err := w.flushData(); err != nil {
			return err
		}
	}
	w.data.add(key, val)
	uk := append([]byte(nil), ikey.UserKey(key)...)
	w.keys = append(w.keys, uk)
	if w.smallest == nil {
		w.smallest = append([]byte(nil), key...)
	}
	w.largest = append(w.largest[:0], key...)
	w.nkv++
	return nil
}

func (w *Writer) flushData() error {
	if w.data.empty() {
		return nil
	}
	sep := append([]byte(nil), w.data.lastKey...)
	blk := w.data.finish()
	n, err := w.f.Write(blk)
	if err != nil {
		return err
	}
	h := blockHandle{Off: w.off, Size: uint64(n)}
	w.off += uint64(n)
	w.index.add(sep, encodeHandle(h))
	w.data.reset()
	return nil
}

func (w *Writer) Finish() (Meta, error) {
	if err := w.flushData(); err != nil {
		return Meta{}, err
	}
	filt := bloom.New(w.keys, BitsPerKey)
	// Filter block = filter bytes || crc32c(filter). The CRC is what makes I7
	// hold for the whole file: without it a flipped bit in the filter only
	// trades false positives for silent false negatives.
	fb := make([]byte, 0, len(filt)+4)
	fb = append(fb, filt...)
	var fcb [4]byte
	binary.LittleEndian.PutUint32(fcb[:], crc32c.Sum(fb))
	fb = append(fb, fcb[:]...)
	fn, err := w.f.Write(fb)
	if err != nil {
		return Meta{}, err
	}
	fh := blockHandle{Off: w.off, Size: uint64(fn)}
	w.off += uint64(fn)

	ib := w.index.finish()
	in, err := w.f.Write(ib)
	if err != nil {
		return Meta{}, err
	}
	ih := blockHandle{Off: w.off, Size: uint64(in)}
	w.off += uint64(in)

	// Footer: filter_handle(16) | index_handle(16) | magic(8) | version(4) |
	// crc32c(4). The CRC covers bytes [0,44) so every footer bit is protected.
	var foot [FooterSize]byte
	binary.LittleEndian.PutUint64(foot[0:8], fh.Off)
	binary.LittleEndian.PutUint64(foot[8:16], fh.Size)
	binary.LittleEndian.PutUint64(foot[16:24], ih.Off)
	binary.LittleEndian.PutUint64(foot[24:32], ih.Size)
	binary.LittleEndian.PutUint64(foot[32:40], Magic)
	binary.LittleEndian.PutUint32(foot[40:44], FormatVersion)
	binary.LittleEndian.PutUint32(foot[44:48], crc32c.Sum(foot[0:44]))
	if _, err := w.f.Write(foot[:]); err != nil {
		return Meta{}, err
	}
	if err := w.f.Sync(); err != nil {
		return Meta{}, err
	}
	if err := w.f.Close(); err != nil {
		return Meta{}, err
	}
	if err := w.fs.Rename(w.tmp, w.final); err != nil {
		return Meta{}, err
	}
	if err := w.fs.SyncDir(w.dir); err != nil {
		return Meta{}, err
	}
	return Meta{
		Num:      w.num,
		Size:     int64(w.off + FooterSize),
		Smallest: append([]byte(nil), w.smallest...),
		Largest:  append([]byte(nil), w.largest...),
		Count:    w.nkv,
	}, nil
}

func (w *Writer) Abort() {
	_ = w.f.Close()
	_ = w.fs.Remove(w.tmp)
}

type Meta struct {
	Num      uint64
	Size     int64
	Smallest []byte
	Largest  []byte
	Count    int
	Level    int
}

type Reader struct {
	fs     fsx.FS
	path   string
	f      fsx.File
	size   int64
	filter bloom.Filter
	index  *blockReader
	filtH  blockHandle
	idxH   blockHandle
}

func Open(fs fsx.FS, dir string, num uint64) (*Reader, error) {
	p := path.Join(dir, Filename(num))
	f, err := fs.Open(p)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() < FooterSize {
		f.Close()
		return nil, ErrCorruptTable
	}
	foot := make([]byte, FooterSize)
	if _, err := f.ReadAt(foot, st.Size()-FooterSize); err != nil {
		f.Close()
		return nil, err
	}
	magic := binary.LittleEndian.Uint64(foot[32:40])
	if magic != Magic {
		f.Close()
		return nil, ErrCorruptTable
	}
	if binary.LittleEndian.Uint32(foot[40:44]) != FormatVersion {
		f.Close()
		return nil, ErrCorruptTable
	}
	if crc32c.Sum(foot[0:44]) != binary.LittleEndian.Uint32(foot[44:48]) {
		f.Close()
		return nil, ErrCorruptTable
	}
	fh := blockHandle{
		Off:  binary.LittleEndian.Uint64(foot[0:8]),
		Size: binary.LittleEndian.Uint64(foot[8:16]),
	}
	ih := blockHandle{
		Off:  binary.LittleEndian.Uint64(foot[16:24]),
		Size: binary.LittleEndian.Uint64(foot[24:32]),
	}
	r := &Reader{fs: fs, path: p, f: f, size: st.Size(), filtH: fh, idxH: ih}
	fb, err := r.readRaw(fh)
	if err != nil {
		f.Close()
		return nil, err
	}
	if len(fb) < 4 || crc32c.Sum(fb[:len(fb)-4]) != binary.LittleEndian.Uint32(fb[len(fb)-4:]) {
		f.Close()
		return nil, ErrCorruptTable
	}
	r.filter = bloom.Filter(fb[:len(fb)-4])
	ib, err := r.readRaw(ih)
	if err != nil {
		f.Close()
		return nil, err
	}
	br, err := parseBlock(ib)
	if err != nil {
		f.Close()
		return nil, err
	}
	r.index = br
	return r, nil
}

func (r *Reader) readRaw(h blockHandle) ([]byte, error) {
	buf := make([]byte, h.Size)
	_, err := r.f.ReadAt(buf, int64(h.Off))
	return buf, err
}

func (r *Reader) readBlock(h blockHandle) (*blockReader, error) {
	raw, err := r.readRaw(h)
	if err != nil {
		return nil, err
	}
	return parseBlock(raw)
}

func (r *Reader) MayContain(user []byte) bool {
	if len(r.filter) == 0 {
		return true
	}
	return r.filter.MayContain(user)
}

func (r *Reader) Get(user []byte, seq uint64) ([]byte, bool, bool, error) {
	if !r.MayContain(user) {
		return nil, false, false, nil
	}
	target := ikey.Encode(user, seq, ikey.KindValue)
	it := r.index.iter()
	it.Seek(target)
	if !it.Valid() {
		it.SeekToFirst()
		if !it.Valid() {
			return nil, false, false, nil
		}
		var last []byte
		for it.Valid() {
			last = append(last[:0], it.Value()...)
			it.Next()
		}
		h, err := decodeHandle(last)
		if err != nil {
			return nil, false, false, err
		}
		return r.getInBlock(h, user, seq)
	}
	for it.Valid() {
		h, err := decodeHandle(it.Value())
		if err != nil {
			return nil, false, false, err
		}
		v, ok, del, err := r.getInBlock(h, user, seq)
		if err != nil || ok {
			return v, ok, del, err
		}
		it.Next()
	}
	return nil, false, false, nil
}

func (r *Reader) getInBlock(h blockHandle, user []byte, seq uint64) ([]byte, bool, bool, error) {
	br, err := r.readBlock(h)
	if err != nil {
		return nil, false, false, err
	}
	target := ikey.Encode(user, seq, ikey.KindValue)
	it := br.iter()
	it.Seek(target)
	for it.Valid() {
		if ikey.CompareUser(it.Key(), target) != 0 {
			return nil, false, false, it.Err()
		}
		kseq := ikey.Seq(it.Key())
		if kseq > seq {
			it.Next()
			continue
		}
		if ikey.KindOf(it.Key()) == ikey.KindDelete {
			return nil, true, true, nil
		}
		v := append([]byte(nil), it.Value()...)
		return v, true, false, nil
	}
	return nil, false, false, it.Err()
}

func (r *Reader) NewIterator() *TableIter {
	return &TableIter{r: r}
}

func (r *Reader) Close() error { return r.f.Close() }

func (r *Reader) ChecksumOK() error {
	buf := make([]byte, r.size)
	if _, err := r.f.ReadAt(buf, 0); err != nil && err != io.EOF {
		return err
	}
	if int64(len(buf)) < FooterSize {
		return ErrCorruptTable
	}
	foot := buf[len(buf)-FooterSize:]
	if binary.LittleEndian.Uint64(foot[32:40]) != Magic {
		return ErrCorruptTable
	}
	if binary.LittleEndian.Uint32(foot[40:44]) != FormatVersion {
		return ErrCorruptTable
	}
	if crc32c.Sum(foot[0:44]) != binary.LittleEndian.Uint32(foot[44:48]) {
		return ErrCorruptTable
	}
	ih := blockHandle{
		Off:  binary.LittleEndian.Uint64(foot[16:24]),
		Size: binary.LittleEndian.Uint64(foot[24:32]),
	}
	fh := blockHandle{
		Off:  binary.LittleEndian.Uint64(foot[0:8]),
		Size: binary.LittleEndian.Uint64(foot[8:16]),
	}
	limit := uint64(len(buf) - FooterSize)
	if ih.Off+ih.Size > limit || fh.Off+fh.Size > limit {
		return ErrCorruptTable
	}
	if fh.Size < 4 {
		return ErrCorruptTable
	}
	fblk := buf[fh.Off : fh.Off+fh.Size]
	if crc32c.Sum(fblk[:len(fblk)-4]) != binary.LittleEndian.Uint32(fblk[len(fblk)-4:]) {
		return ErrCorruptTable
	}
	idx, err := parseBlock(buf[ih.Off : ih.Off+ih.Size])
	if err != nil {
		return err
	}
	it := idx.iter()
	it.SeekToFirst()
	for it.Valid() {
		h, err := decodeHandle(it.Value())
		if err != nil {
			return err
		}
		if h.Off+h.Size > limit {
			return ErrCorruptTable
		}
		if _, err := parseBlock(buf[h.Off : h.Off+h.Size]); err != nil {
			return err
		}
		it.Next()
	}
	return it.Err()
}

type TableIter struct {
	r     *Reader
	idx   *blockIter
	blk   *blockIter
	valid bool
	err   error
}

func (it *TableIter) loadBlock() bool {
	if !it.idx.Valid() {
		it.valid = false
		return false
	}
	h, err := decodeHandle(it.idx.Value())
	if err != nil {
		it.err = err
		it.valid = false
		return false
	}
	br, err := it.r.readBlock(h)
	if err != nil {
		it.err = err
		it.valid = false
		return false
	}
	it.blk = br.iter()
	it.blk.SeekToFirst()
	it.valid = it.blk.Valid()
	return it.valid
}

func (it *TableIter) SeekToFirst() {
	it.idx = it.r.index.iter()
	it.idx.SeekToFirst()
	it.loadBlock()
}

func (it *TableIter) Seek(key []byte) {
	it.idx = it.r.index.iter()
	it.idx.Seek(key)
	if !it.idx.Valid() {
		it.idx.SeekToFirst()
		if !it.idx.Valid() {
			it.valid = false
			return
		}
		var lastH []byte
		for it.idx.Valid() {
			lastH = append(lastH[:0], it.idx.Value()...)
			it.idx.Next()
		}
		h, err := decodeHandle(lastH)
		if err != nil {
			it.err = err
			it.valid = false
			return
		}
		br, err := it.r.readBlock(h)
		if err != nil {
			it.err = err
			it.valid = false
			return
		}
		it.blk = br.iter()
		it.blk.Seek(key)
		it.valid = it.blk.Valid()
		it.idx = it.r.index.iter()
		it.idx.SeekToFirst()
		for it.idx.Valid() {
			it.idx.Next()
		}
		return
	}
	if !it.loadBlock() {
		return
	}
	it.blk.Seek(key)
	if it.blk.Valid() {
		it.valid = true
		return
	}
	it.idx.Next()
	it.loadBlock()
}

func (it *TableIter) Next() {
	if it.blk != nil && it.blk.Valid() {
		it.blk.Next()
		if it.blk.Valid() {
			it.valid = true
			return
		}
	}
	if it.idx != nil {
		it.idx.Next()
		it.loadBlock()
	}
}

func (it *TableIter) Valid() bool { return it.valid }
func (it *TableIter) Key() []byte {
	if it.blk == nil {
		return nil
	}
	return it.blk.Key()
}
func (it *TableIter) Value() []byte {
	if it.blk == nil {
		return nil
	}
	return it.blk.Value()
}
func (it *TableIter) Err() error { return it.err }
