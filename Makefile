.PHONY: test gates gates-full bench kill9

test:
	go test ./...

gates:
	bash scripts/gates.sh

gates-full:
	LSM_FULL=1 bash scripts/gates.sh

bench:
	go run ./cmd/kvbench -mode seq -n 8000 -vlen 128
	go run ./cmd/kvbench -mode rand -n 4000 -vlen 128

kill9:
	go run ./cmd/crashkill -trials 5 -n 80
