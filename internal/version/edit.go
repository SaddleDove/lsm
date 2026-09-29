package version

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"

	"lsm/internal/crc32c"
	"lsm/internal/fsx"
	"lsm/internal/sstable"
)

const (
	tagComparator  = 1
	tagLogNumber   = 2
	tagNextFile    = 3
	tagLastSeq     = 4
	tagDeletedFile = 6
	tagNewFile     = 7
	tagPrevLog     = 9
	tagMinSnapshot = 10
	CurrentName    = "CURRENT"
	ManifestPrefix = "MANIFEST-"
)

var ErrCorruptManifest = errors.New("version: corrupt manifest")

type FileMeta struct {
	Num      uint64
	Level    int
	Size     int64
	Smallest []byte
	Largest  []byte
	Count    int
}

func FromSST(m sstable.Meta, level int) FileMeta {
	return FileMeta{
		Num: m.Num, Level: level, Size: m.Size,
		Smallest: append([]byte(nil), m.Smallest...),
		Largest:  append([]byte(nil), m.Largest...),
		Count:    m.Count,
	}
}

type Edit struct {
	LogNumber   *uint64
	NextFile    *uint64
	LastSeq     *uint64
	PrevLog     *uint64
	MinSnapshot *uint64
	Add         []FileMeta
	Del         []struct {
		Level int
		Num   uint64
	}
}

func (e *Edit) SetLogNumber(n uint64)   { e.LogNumber = &n }
func (e *Edit) SetNextFile(n uint64)    { e.NextFile = &n }
func (e *Edit) SetLastSeq(n uint64)     { e.LastSeq = &n }
func (e *Edit) SetPrevLog(n uint64)     { e.PrevLog = &n }
func (e *Edit) SetMinSnapshot(n uint64) { e.MinSnapshot = &n }

func (e *Edit) AddFile(f FileMeta) {
	e.Add = append(e.Add, f)
}

func (e *Edit) DeleteFile(level int, num uint64) {
	e.Del = append(e.Del, struct {
		Level int
		Num   uint64
	}{level, num})
}

func putUvarint(dst []byte, x uint64) []byte {
	for x >= 0x80 {
		dst = append(dst, byte(x)|0x80)
		x >>= 7
	}
	return append(dst, byte(x))
}

func getUvarint(b []byte) (uint64, []byte, error) {
	var v uint64
	s := uint(0)
	for n := 0; n < len(b); n++ {
		if b[n] < 0x80 {
			v |= uint64(b[n]) << s
			return v, b[n+1:], nil
		}
		v |= uint64(b[n]&0x7f) << s
		s += 7
	}
	return 0, nil, ErrCorruptManifest
}

func putBytes(dst, s []byte) []byte {
	dst = putUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func getBytes(b []byte) ([]byte, []byte, error) {
	n, rest, err := getUvarint(b)
	if err != nil {
		return nil, nil, err
	}
	if uint64(len(rest)) < n {
		return nil, nil, ErrCorruptManifest
	}
	return append([]byte(nil), rest[:n]...), rest[n:], nil
}

func Encode(e *Edit) []byte {
	var b []byte
	if e.LogNumber != nil {
		b = append(b, tagLogNumber)
		b = putUvarint(b, *e.LogNumber)
	}
	if e.PrevLog != nil {
		b = append(b, tagPrevLog)
		b = putUvarint(b, *e.PrevLog)
	}
	if e.NextFile != nil {
		b = append(b, tagNextFile)
		b = putUvarint(b, *e.NextFile)
	}
	if e.LastSeq != nil {
		b = append(b, tagLastSeq)
		b = putUvarint(b, *e.LastSeq)
	}
	if e.MinSnapshot != nil {
		b = append(b, tagMinSnapshot)
		b = putUvarint(b, *e.MinSnapshot)
	}
	for _, d := range e.Del {
		b = append(b, tagDeletedFile)
		b = putUvarint(b, uint64(d.Level))
		b = putUvarint(b, d.Num)
	}
	for _, f := range e.Add {
		b = append(b, tagNewFile)
		b = putUvarint(b, uint64(f.Level))
		b = putUvarint(b, f.Num)
		b = putUvarint(b, uint64(f.Size))
		b = putBytes(b, f.Smallest)
		b = putBytes(b, f.Largest)
		b = putUvarint(b, uint64(f.Count))
	}
	return b
}

func Decode(b []byte) (*Edit, error) {
	e := &Edit{}
	for len(b) > 0 {
		tag := b[0]
		b = b[1:]
		var err error
		switch tag {
		case tagLogNumber:
			var v uint64
			v, b, err = getUvarint(b)
			e.SetLogNumber(v)
		case tagPrevLog:
			var v uint64
			v, b, err = getUvarint(b)
			e.SetPrevLog(v)
		case tagNextFile:
			var v uint64
			v, b, err = getUvarint(b)
			e.SetNextFile(v)
		case tagLastSeq:
			var v uint64
			v, b, err = getUvarint(b)
			e.SetLastSeq(v)
		case tagMinSnapshot:
			var v uint64
			v, b, err = getUvarint(b)
			e.SetMinSnapshot(v)
		case tagDeletedFile:
			var lv, num uint64
			lv, b, err = getUvarint(b)
			if err != nil {
				return nil, err
			}
			num, b, err = getUvarint(b)
			e.DeleteFile(int(lv), num)
		case tagNewFile:
			var lv, num, sz, cnt uint64
			lv, b, err = getUvarint(b)
			if err != nil {
				return nil, err
			}
			num, b, err = getUvarint(b)
			if err != nil {
				return nil, err
			}
			sz, b, err = getUvarint(b)
			if err != nil {
				return nil, err
			}
			var sm, lg []byte
			sm, b, err = getBytes(b)
			if err != nil {
				return nil, err
			}
			lg, b, err = getBytes(b)
			if err != nil {
				return nil, err
			}
			cnt, b, err = getUvarint(b)
			e.AddFile(FileMeta{Num: num, Level: int(lv), Size: int64(sz), Smallest: sm, Largest: lg, Count: int(cnt)})
		default:
			return nil, ErrCorruptManifest
		}
		if err != nil {
			return nil, err
		}
	}
	return e, nil
}

func EncodeRecord(e *Edit) []byte {
	body := Encode(e)
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32c.Sum(body))
	return append(hdr, body...)
}

func DecodeRecord(r io.Reader) (*Edit, error) {
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[0:4])
	want := binary.LittleEndian.Uint32(hdr[4:8])
	if n > 16<<20 {
		return nil, ErrCorruptManifest
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return nil, ErrCorruptManifest
		}
		return nil, err
	}
	if crc32c.Sum(body) != want {
		return nil, ErrCorruptManifest
	}
	return Decode(body)
}

func ManifestName(num uint64) string {
	return fmt.Sprintf("MANIFEST-%06d", num)
}

func ReadCurrent(fs fsx.FS, dir string) (string, error) {
	f, err := fs.Open(path.Join(dir, CurrentName))
	if err != nil {
		return "", err
	}
	defer f.Close()
	buf := make([]byte, 256)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return "", err
	}
	s := string(buf[:n])
	if len(s) > 0 && s[len(s)-1] == '\n' {
		s = s[:len(s)-1]
	}
	if s == "" {
		return "", ErrCorruptManifest
	}
	return s, nil
}

func WriteCurrent(fs fsx.FS, dir, manifest string) error {
	tmp := path.Join(dir, "CURRENT.tmp")
	f, err := fs.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write([]byte(manifest + "\n")); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fs.Rename(tmp, path.Join(dir, CurrentName)); err != nil {
		return err
	}
	return fs.SyncDir(dir)
}

type Set struct {
	Levels      [][]FileMeta
	LogNumber   uint64
	NextFile    uint64
	LastSeq     uint64
	PrevLog     uint64
	MinSnapshot uint64
	Manifest    uint64
}

func NewSet(maxLevel int) *Set {
	return &Set{Levels: make([][]FileMeta, maxLevel), NextFile: 1, LogNumber: 1}
}

func (s *Set) Clone() *Set {
	n := &Set{
		Levels:      make([][]FileMeta, len(s.Levels)),
		LogNumber:   s.LogNumber,
		NextFile:    s.NextFile,
		LastSeq:     s.LastSeq,
		PrevLog:     s.PrevLog,
		MinSnapshot: s.MinSnapshot,
		Manifest:    s.Manifest,
	}
	for i, lv := range s.Levels {
		n.Levels[i] = append([]FileMeta(nil), lv...)
	}
	return n
}

func (s *Set) Apply(e *Edit) {
	if e.LogNumber != nil {
		s.LogNumber = *e.LogNumber
	}
	if e.NextFile != nil && *e.NextFile > s.NextFile {
		s.NextFile = *e.NextFile
	}
	if e.LastSeq != nil && *e.LastSeq > s.LastSeq {
		s.LastSeq = *e.LastSeq
	}
	if e.PrevLog != nil {
		s.PrevLog = *e.PrevLog
	}
	if e.MinSnapshot != nil {
		s.MinSnapshot = *e.MinSnapshot
	}
	del := map[uint64]bool{}
	for _, d := range e.Del {
		del[d.Num] = true
		lv := s.Levels[d.Level]
		out := lv[:0]
		for _, f := range lv {
			if f.Num != d.Num {
				out = append(out, f)
			}
		}
		s.Levels[d.Level] = out
	}
	for _, f := range e.Add {
		if del[f.Num] {
			continue
		}
		if f.Level >= len(s.Levels) {
			n := make([][]FileMeta, f.Level+1)
			copy(n, s.Levels)
			s.Levels = n
		}
		s.Levels[f.Level] = append(s.Levels[f.Level], f)
	}
	for i := range s.Levels {
		sort.Slice(s.Levels[i], func(a, b int) bool {
			if i == 0 {
				return s.Levels[i][a].Num < s.Levels[i][b].Num
			}
			return string(s.Levels[i][a].Smallest) < string(s.Levels[i][b].Smallest)
		})
	}
}

func (s *Set) AllFiles() []FileMeta {
	var out []FileMeta
	for _, lv := range s.Levels {
		out = append(out, lv...)
	}
	return out
}

func (s *Set) FileNums() map[uint64]bool {
	m := map[uint64]bool{}
	for _, f := range s.AllFiles() {
		m[f.Num] = true
	}
	return m
}

type Log struct {
	fs   fsx.FS
	dir  string
	num  uint64
	f    fsx.File
	path string
}

func CreateManifest(fs fsx.FS, dir string, num uint64, snap *Set) (*Log, error) {
	name := ManifestName(num)
	p := path.Join(dir, name)
	f, err := fs.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	l := &Log{fs: fs, dir: dir, num: num, f: f, path: p}
	e := &Edit{}
	e.SetLogNumber(snap.LogNumber)
	e.SetNextFile(snap.NextFile)
	e.SetLastSeq(snap.LastSeq)
	e.SetPrevLog(snap.PrevLog)
	e.SetMinSnapshot(snap.MinSnapshot)
	for _, fmeta := range snap.AllFiles() {
		e.AddFile(fmeta)
	}
	if err := l.Append(e); err != nil {
		f.Close()
		return nil, err
	}
	if err := WriteCurrent(fs, dir, name); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// HasManifest reports whether any MANIFEST-* file exists in dir. It is used to
// distinguish a *fresh* directory (no CURRENT, no MANIFEST -> bootstrap) from a
// *damaged* one (MANIFEST present but CURRENT missing -> fail fast).
func HasManifest(fs fsx.FS, dir string) bool {
	ents, err := fs.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range ents {
		var n uint64
		if _, err := fmt.Sscanf(e.Name(), "MANIFEST-%d", &n); err == nil {
			return true
		}
	}
	return false
}

// OpenManifest reads CURRENT and replays the manifest it points at. Per
// DESIGN 8.2 c8 / 317 ("不允许猜"), a missing CURRENT, a CURRENT that does not
// name an existing MANIFEST, or a corrupt manifest record is a hard error: we
// never guess at a different manifest and we never silently truncate a bad
// record. The only tolerated end-of-file is a clean record boundary.
func OpenManifest(fs fsx.FS, dir string) (*Log, *Set, error) {
	name, err := ReadCurrent(fs, dir)
	if err != nil {
		return nil, nil, err
	}
	var num uint64
	if _, err := fmt.Sscanf(name, "MANIFEST-%d", &num); err != nil {
		return nil, nil, ErrCorruptManifest
	}
	if _, err := fs.Stat(path.Join(dir, name)); err != nil {
		return nil, nil, ErrCorruptManifest
	}
	f, err := fs.OpenFile(path.Join(dir, name), os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, err
	}
	set := NewSet(7)
	set.Manifest = num
	for {
		e, err := DecodeRecord(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			return nil, nil, ErrCorruptManifest
		}
		set.Apply(e)
	}
	l := &Log{fs: fs, dir: dir, num: num, f: f, path: path.Join(dir, name)}
	return l, set, nil
}

func (l *Log) Append(e *Edit) error {
	rec := EncodeRecord(e)
	if _, err := l.f.Write(rec); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	return l.fs.SyncDir(l.dir)
}

func (l *Log) Close() error { return l.f.Close() }

func (l *Log) Num() uint64 { return l.num }
