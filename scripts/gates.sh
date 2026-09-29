#!/bin/bash
set -euo pipefail

# Run the evidence gates and write testdata/report.json + PROGRESS.md.
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

FULL=0
if [ "${LSM_FULL:-}" = "1" ]; then
  FULL=1
fi

# Build first so go run is warm.
go test ./internal/...

if [ "$FULL" = "1" ]; then
  go run ./cmd/report -root "$ROOT" -out "$ROOT/testdata" -full
else
  go run ./cmd/report -root "$ROOT" -out "$ROOT/testdata"
fi
