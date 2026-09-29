package crc32c

import "hash/crc32"

var table = crc32.MakeTable(crc32.Castagnoli)

func Sum(b []byte) uint32 {
	return crc32.Checksum(b, table)
}
