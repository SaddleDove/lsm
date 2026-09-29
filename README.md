# LSM KV engine

[![ci](https://github.com/SaddleDove/lsm/actions/workflows/ci.yml/badge.svg)](https://github.com/SaddleDove/lsm/actions/workflows/ci.yml)

Single-process, single-node key-value store in Go with zero external dependencies (standard library only).

- Write path: `WriteBatch -> WAL -> skiplist memtable -> SSTable (L0) -> leveled compaction (L1..L6) -> MANIFEST`.
- Read path: point `Get` and range `Scan(start,end)`, both served from the active memtable, the immutable memtable and the SSTables through a k-way merge (`MergingIterator`) that applies snapshot sequence filtering and tombstone masking.

## Invariants

1. Durable before visible: a write is fsynced to the WAL (per sync policy) before it enters the memtable.
2. Files before accounting: SSTable tmp -> fsync -> rename -> fsync(dir) -> MANIFEST edit. Old files are deleted only after the edit is durable.
3. Physical I/O is counted at the stack bottom, in `internal/fsx`. Every create/write/read/fsync/rename/remove goes through that wrapper.

## Read path

- `Get(k)`: active memtable -> immutable memtable -> L0 (newest file first) -> L1..L6; Bloom is consulted on L>=1.
- `Scan(start, end)`: merges active memtable + immutable memtable + every overlapping SSTable (L0 newest->oldest, then L1..L6). `start` is inclusive, `end` exclusive; `nil` means unbounded. Entries newer than the snapshot are skipped, a tombstone masks older versions of its key, and user-visible tombstones are never emitted. `Snapshot.Scan` scans as of the snapshot's sequence.

## Package map

- `internal/fsx` — MemFS + OSFS + crash injector (crash-at-op-N and torn-tail) + stats (`bytes_written`, `read_bytes`, `bytes_synced`, `fsyncs`, `renames`, ...)
- `internal/ikey` — user_key || seq(7B) || kind, seq descending
- `internal/wal` — length + crc32c frames, torn-tail truncation on replay, 64 MB payload cap enforced by writer and reader
- `internal/memtable` — skiplist
- `internal/sstable` — prefix-compressed blocks, restart points, Bloom (10 bits/key, k=7), filter CRC, 48-byte footer (filter | index | magic | version | crc32c)
- `internal/version` — MANIFEST / VersionEdit / CURRENT (fail-fast on a missing or corrupt CURRENT)
- `internal/engine` — DB, flush, leveled compaction, recovery, snapshots, scan

## Build / test

```
go test ./...
make gates
```

`scripts/gates.sh` (via `cmd/report`) writes `testdata/report.json` and `PROGRESS.md`. Do not edit those two files by hand: the run also checks the freshly generated evidence against the committed copies (ignoring timestamps/durations) and exits non-zero on drift.

Default gates already run the 2000+ point deterministic crash sweep. `LSM_FULL=1 make gates-full` additionally runs:

- 200 `kill -9` trials (default gate runs 5 trials x 80 keys and reports how many landed mid-write);
- a ~100 MB WAL recovery benchmark that enforces the 2 s bound.

## Crash matrix (c1-c10)

| id | injection | expected |
|---|---|---|
| c1 | WAL torn tail | torn record dropped, all prior full records replay |
| c2 | crash after ack | acked keys survive |
| c3 | crash mid-batch | all or nothing |
| c4 | crash during flush tmp write | data still in the WAL |
| c5 | SST renamed, MANIFEST edit not durable | orphan SST not served, collected on reopen |
| c6 | MANIFEST edit durable, old WAL not yet dropped | replay skips persisted seqs, keys served from the SST |
| c7 | crash mid-compaction | old files remain until the edit is durable; manifest never references a missing file |
| c8 | compaction complete / stray files | no tmp or orphan SST; hand-made `.sst`/`.tmp`/`.log`/`MANIFEST`/`CURRENT.tmp` orphans collected |
| c9 | CURRENT torn | fail fast with an error (no guessing at another manifest) |
| c10 | recover twice | identical sequence, values and file set |

## Limits / thresholds

- WA sequential fill <= 8
- WA random overwrite <= 25
- space amp <= 1.5
- read amp <= 3 SST reads/Get (Bloom on)
- Bloom FPR <= 1.2x theoretical (10 bits/key, k=7)
- recovery <= 2 s at the small default scale and, in `LSM_FULL=1` mode, at ~100 MB WAL

## Write-amplification model

`WA ~ 1(WAL) + 1(flush) + T*(levels-1)` with `T=10`, where `levels` is the deepest level the data actually reached, measured from the live version (not a per-mode constant). This is an asymptotic, steady-state estimate; at the small scale of the default benchmark it is not tight, so the gates assert only the absolute bounds and the report prints the measured model and the deviation. The deviation is reported, not asserted.

## Scope and simplifications

This is a compact, single-process engine, and a few pieces are deliberately simpler than a production LSM:

- Leveled compaction is simplified: one output file per compaction (no grandparent-overlap output splitting), L0 chooses its inputs by newest file, and L+1 (L>=1) picks the first overlapping file rather than rotating through a `compact_pointer`.
- `Snapshot` holds only a sequence number, not a live `Version` reference. Compaction still respects the oldest open snapshot when deciding which versions to drop.
- On replay, a WAL record with a bad CRC is treated as a torn tail (everything from that point in the file is dropped), per the format rules in the design; SSTable and MANIFEST corruption is reported as an error instead.
- A missing or corrupt `CURRENT`/MANIFEST fails the open with an error; the engine never guesses at another manifest.
- `Scan` materialises its range under the DB lock and returns a slice-backed iterator (the engine is single-writer with no background threads), so a scan is a fully consistent snapshot at the cost of memory proportional to the range.
- Only a subset of the design's write-amplification counters is implemented (`user_bytes`, `wal_bytes`, `flush_bytes`, `manifest_bytes`, `read_bytes`, `sstable_reads`, `fsyncs`, `renames`).

None of these affects the invariants or the gated thresholds above; they bound the feature surface of the engine.
