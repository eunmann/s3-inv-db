# Inventory pipeline benchmark sweep

Branch-per-finding A/B comparison against `bench/baseline`.

Each variant branch contains:
- An implementation change for one finding
- Same benchmark code as baseline
- A `bench-results/<branch>.txt` capturing `go test -bench ... -benchmem` output

Use `benchstat bench-results/baseline.txt bench-results/<variant>.txt` to compare.

## Branches

| Branch | Finding | Priority |
|---|---|---|
| bench/baseline | reference impl, all baseline benchmarks | — |
| **Correctness** | | |
| test/tier-backfill-regression | regression test for tier-stats off-by-one claim | C |
| test/runfile-header-crash | simulate partial header rewrite | C |
| test/substring-retention | measure heap retention from key buffers | C |
| **Ingestion speed** | | |
| bench/clone-on-miss | strings.Clone only on first-insertion | I1 |
| bench/typed-merge-heap | typed heap, no container/heap interface{} | I2 |
| bench/dedicated-flush | async aggregator flush goroutine | I3 |
| bench/pooled-zstd | pool zstd encoder/decoder across run files | I4 |
| bench/madvise-hints | MADV_RANDOM/SEQUENTIAL on columnar mmaps | I5 |
| bench/header-alignment | 24-byte (aligned) vs 20-byte header | I6 |
| bench/single-pass-drain | combined iterate+delete in Drain | I7 |
| bench/sort-jobs-by-size | manifest files sorted largest-first | I8 |
| bench/csv-handrolled | hand-rolled CSV parser vs encoding/csv | I9 |
| bench/pool-prefixrow-merge | sync.Pool for PrefixRow during merge | I10 |
| **Disk size** | | |
| bench/tier-bitmap-runfile | sparse-tier encoding in run-file format | D1 |
