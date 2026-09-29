package ikey

import (
	"bytes"
	"encoding/binary"
)

type Kind uint8

const (
	KindDelete Kind = 0
	KindValue  Kind = 1
)

const TrailerSize = 8

func Encode(user []byte, seq uint64, kind Kind) []byte {
	out := make([]byte, len(user)+TrailerSize)
	copy(out, user)
	binary.BigEndian.PutUint64(out[len(user):], Pack(seq, kind))
	return out
}

func Pack(seq uint64, kind Kind) uint64 {
	return (seq << 8) | uint64(kind)
}

func Unpack(trailer uint64) (seq uint64, kind Kind) {
	return trailer >> 8, Kind(trailer & 0xff)
}

func UserKey(ikey []byte) []byte {
	if len(ikey) < TrailerSize {
		return ikey
	}
	return ikey[:len(ikey)-TrailerSize]
}

func Trailer(ikey []byte) uint64 {
	if len(ikey) < TrailerSize {
		return 0
	}
	return binary.BigEndian.Uint64(ikey[len(ikey)-TrailerSize:])
}

func Seq(ikey []byte) uint64 {
	s, _ := Unpack(Trailer(ikey))
	return s
}

func KindOf(ikey []byte) Kind {
	_, k := Unpack(Trailer(ikey))
	return k
}

func Compare(a, b []byte) int {
	ua, ub := UserKey(a), UserKey(b)
	if c := bytes.Compare(ua, ub); c != 0 {
		return c
	}
	ta, tb := Trailer(a), Trailer(b)
	if ta > tb {
		return -1
	}
	if ta < tb {
		return 1
	}
	return 0
}

func CompareUser(a, b []byte) int {
	return bytes.Compare(UserKey(a), UserKey(b))
}

func MaxSeq() uint64 {
	return (1 << 56) - 1
}

func RangeMeta(smallest, largest, ukey []byte) bool {
	if bytes.Compare(ukey, UserKey(smallest)) < 0 {
		return false
	}
	if bytes.Compare(ukey, UserKey(largest)) > 0 {
		return false
	}
	return true
}
