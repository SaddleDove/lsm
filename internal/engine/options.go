package engine

import "lsm/internal/fsx"

const (
	DefaultMemtableBytes   = 4 << 20
	DefaultMaxLevel        = 7
	DefaultLevelMultiplier = 10
	DefaultL0Trigger       = 4
	DefaultBlockSize       = 4096
	DefaultBloomBits       = 10
)

type SyncMode int

const (
	SyncEveryWrite SyncMode = iota
	SyncGroup
	SyncNone
)

type Options struct {
	Dir              string
	FS               fsx.FS
	MemtableBytes    int64
	MaxLevel         int
	LevelMultiplier  int
	L0CompactionTrig int
	BlockSize        int
	BloomBits        int
	Sync             SyncMode
	ReadOnly         bool
}

func (o *Options) fill() {
	if o.MemtableBytes <= 0 {
		o.MemtableBytes = DefaultMemtableBytes
	}
	if o.MaxLevel <= 0 {
		o.MaxLevel = DefaultMaxLevel
	}
	if o.LevelMultiplier <= 0 {
		o.LevelMultiplier = DefaultLevelMultiplier
	}
	if o.L0CompactionTrig <= 0 {
		o.L0CompactionTrig = DefaultL0Trigger
	}
	if o.BlockSize <= 0 {
		o.BlockSize = DefaultBlockSize
	}
	if o.BloomBits <= 0 {
		o.BloomBits = DefaultBloomBits
	}
}

type WriteOptions struct {
	Sync bool
}

type ReadOptions struct {
	Seq uint64
}

type BatchEntry struct {
	Key   []byte
	Value []byte
	Del   bool
}

type WriteBatch struct {
	Entries []BatchEntry
}

func (b *WriteBatch) Put(k, v []byte) {
	b.Entries = append(b.Entries, BatchEntry{
		Key:   append([]byte(nil), k...),
		Value: append([]byte(nil), v...),
	})
}

func (b *WriteBatch) Delete(k []byte) {
	b.Entries = append(b.Entries, BatchEntry{
		Key: append([]byte(nil), k...),
		Del: true,
	})
}

func (b *WriteBatch) Count() int { return len(b.Entries) }
