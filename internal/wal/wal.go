package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sort"

	"lsm/internal/crc32c"
	"lsm/internal/fsx"
	"lsm/internal/ikey"
)

const (
	headerSize = 8
	KindPut    = ikey.KindValue
	KindDel    = ikey.KindDelete
)

// MaxPayload is the largest single WriteBatch frame accepted. It is the sanity
// bound from DESIGN 4.2 (64 MB) and is enforced identically by the writer
// (which refuses to append and does not ack, so the batch fails loudly) and by
// the reader (which treats anything larger as corruption). It is a variable so
// tests can lower it without allocating 64 MB.
var MaxPayload = 64 << 20

var (
	ErrCorrupt  = errors.New("wal: corrupt record")
	ErrTorn     = errors.New("wal: torn write")
	ErrTooLarge = errors.New("wal: payload exceeds max record size")
)

type Record struct {
	Seq  uint64
	Kind ikey.Kind
	Key  []byte
	Val  []byte
}

func encodeRecord(r Record) []byte {
	n := 8 + 1 + 4 + len(r.Key) + 4 + len(r.Val)
	b := make([]byte, n)
	binary.LittleEndian.PutUint64(b[0:8], r.Seq)
	b[8] = byte(r.Kind)
	binary.LittleEndian.PutUint32(b[9:13], uint32(len(r.Key)))
	copy(b[13:], r.Key)
	off := 13 + len(r.Key)
	binary.LittleEndian.PutUint32(b[off:off+4], uint32(len(r.Val)))
	copy(b[off+4:], r.Val)
	return b
}

func decodeRecord(b []byte) (Record, error) {
	if len(b) < 13 {
		return Record{}, ErrCorrupt
	}
	r := Record{Seq: binary.LittleEndian.Uint64(b[0:8]), Kind: ikey.Kind(b[8])}
	klen := binary.LittleEndian.Uint32(b[9:13])
	if 13+int(klen)+4 > len(b) {
		return Record{}, ErrCorrupt
	}
	r.Key = append([]byte(nil), b[13:13+klen]...)
	off := 13 + int(klen)
	vlen := binary.LittleEndian.Uint32(b[off : off+4])
	if off+4+int(vlen) != len(b) {
		return Record{}, ErrCorrupt
	}
	r.Val = append([]byte(nil), b[off+4:]...)
	return r, nil
}

type Writer struct {
	fs     fsx.FS
	dir    string
	num    uint64
	f      fsx.File
	off    int64
	sync   bool
	closed bool
}

func OpenWriter(fs fsx.FS, dir string, num uint64, sync bool) (*Writer, error) {
	name := Filename(num)
	f, err := fs.OpenFile(path.Join(dir, name), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Writer{fs: fs, dir: dir, num: num, f: f, off: st.Size(), sync: sync}, nil
}

func Filename(num uint64) string {
	return fmt.Sprintf("%06d.log", num)
}

func (w *Writer) Num() uint64 { return w.num }
func (w *Writer) Off() int64  { return w.off }

func (w *Writer) Append(recs []Record) error {
	if w.closed {
		return errors.New("wal: closed")
	}
	var payload []byte
	for _, r := range recs {
		payload = append(payload, encodeRecord(r)...)
		if len(payload) > MaxPayload {
			return ErrTooLarge
		}
	}
	hdr := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	crc := crc32c.Sum(payload)
	binary.LittleEndian.PutUint32(hdr[4:8], crc)
	n, err := w.f.Write(hdr)
	if err != nil {
		return err
	}
	w.off += int64(n)
	n, err = w.f.Write(payload)
	if err != nil {
		return err
	}
	w.off += int64(n)
	if w.sync {
		return w.f.Sync()
	}
	return nil
}

func (w *Writer) Sync() error {
	if w.closed {
		return errors.New("wal: closed")
	}
	return w.f.Sync()
}

func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.f.Close()
}

type Reader struct {
	f   fsx.File
	off int64
}

func OpenReader(fs fsx.FS, dir string, num uint64) (*Reader, error) {
	f, err := fs.Open(path.Join(dir, Filename(num)))
	if err != nil {
		return nil, err
	}
	return &Reader{f: f}, nil
}

func OpenReaderFile(f fsx.File) *Reader {
	return &Reader{f: f}
}

func (r *Reader) Next() ([]Record, error) {
	hdr := make([]byte, headerSize)
	n, err := io.ReadFull(r.f, hdr)
	r.off += int64(n)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	plen := binary.LittleEndian.Uint32(hdr[0:4])
	want := binary.LittleEndian.Uint32(hdr[4:8])
	if plen > uint32(MaxPayload) {
		return nil, ErrCorrupt
	}
	payload := make([]byte, plen)
	n, err = io.ReadFull(r.f, payload)
	r.off += int64(n)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, ErrTorn
	}
	if err != nil {
		return nil, err
	}
	got := crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
	if got != want {
		return nil, ErrCorrupt
	}
	var recs []Record
	rest := payload
	for len(rest) > 0 {
		rec, err := decodeRecordPartial(rest)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec.r)
		rest = rec.rest
	}
	return recs, nil
}

type recRest struct {
	r    Record
	rest []byte
}

func decodeRecordPartial(b []byte) (recRest, error) {
	if len(b) < 13 {
		return recRest{}, ErrCorrupt
	}
	r := Record{Seq: binary.LittleEndian.Uint64(b[0:8]), Kind: ikey.Kind(b[8])}
	klen := int(binary.LittleEndian.Uint32(b[9:13]))
	if 13+klen+4 > len(b) {
		return recRest{}, ErrCorrupt
	}
	r.Key = append([]byte(nil), b[13:13+klen]...)
	off := 13 + klen
	vlen := int(binary.LittleEndian.Uint32(b[off : off+4]))
	if off+4+vlen > len(b) {
		return recRest{}, ErrCorrupt
	}
	r.Val = append([]byte(nil), b[off+4:off+4+vlen]...)
	return recRest{r: r, rest: b[off+4+vlen:]}, nil
}

func (r *Reader) Close() error { return r.f.Close() }

func List(fs fsx.FS, dir string) ([]uint64, error) {
	ents, err := fs.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var nums []uint64
	for _, e := range ents {
		var n uint64
		if _, err := fmt.Sscanf(e.Name(), "%d.log", &n); err == nil {
			nums = append(nums, n)
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

func Replay(fs fsx.FS, dir string, minSeq uint64, fn func(Record) error) (last uint64, torn bool, err error) {
	nums, err := List(fs, dir)
	if err != nil {
		return 0, false, err
	}
	last = minSeq
	for _, num := range nums {
		rd, err := OpenReader(fs, dir, num)
		if err != nil {
			return last, false, err
		}
		for {
			recs, err := rd.Next()
			if err == io.EOF {
				break
			}
			if err == ErrTorn || err == ErrCorrupt {
				torn = true
				break
			}
			if err != nil {
				rd.Close()
				return last, torn, err
			}
			for _, rec := range recs {
				if rec.Seq <= minSeq {
					continue
				}
				if err := fn(rec); err != nil {
					rd.Close()
					return last, torn, err
				}
				if rec.Seq > last {
					last = rec.Seq
				}
			}
		}
		rd.Close()
	}
	return last, torn, nil
}
