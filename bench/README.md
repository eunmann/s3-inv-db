# Benchmarks

Benchmarks gate **performance changes**, not every PR. CI never runs
them: shared runners are too noisy to judge a few percent, and most
changes cannot move performance. Run the gate locally when a change
touches a hot path (ingest, aggregation, spill/merge, index build, the
query path) and cite its result in the commit or PR.

## The A/B gate

```sh
make bench-ab BASE=main            # ROUNDS=8 SUITE=bench/suite.txt
```

[`scripts/bench-ab.sh`](../scripts/bench-ab.sh) builds the test binaries
for `BASE` (in a temporary worktree) and for the working tree, then runs
every entry of [`suite.txt`](suite.txt) `ROUNDS` times, alternating which
side runs first so machine drift hits both sides equally. Base and head
always run on the same machine in the same session, so no stored
baseline can go stale with hardware. `benchstat` compares them, and the
script fails on a statistically significant (p < 0.05) regression beyond:

| Metric | Threshold |
|---|---|
| `sec/op` | `BENCH_TIME_PCT` (default 5%) |
| `B/op`, `allocs/op`, `disk_B` | `BENCH_MEM_PCT` (default 5%) |
| `peak_heap_B` (sampled, so noisier) | `BENCH_HEAP_PCT` (default 15%) |

Results land in `bench/ab/<timestamp>/` (gitignored): raw `base.txt` /
`head.txt`, `report.txt` and `report.csv`. Entries tagged `integration`
need a local S3 server; the script starts one in Docker unless
`AWS_ENDPOINT_URL_S3` is already set.

A regression is merged only with an explicit justification in the
commit message (CLAUDE.md, Performance & Benchmarks).

## The suite

Every entry does milliseconds to seconds of work per op on fixed,
seeded inputs, so repeated rounds agree:

- `internal/loader` `BenchmarkPipeline` at 1M objects (realistic and
  deep-pyramid shapes): the real `Pipeline.Run` end to end through the
  local S3 server. Seconds per op; also reports `peak_heap_B` and
  `disk_B`.
- `pkg/indexread` `BenchmarkGate*`: 100k lookups, prefix
  reconstructions or tier breakdowns, or 1k children + top-10 queries
  per op, over one 1M-object multi-tier index.

When a change touches a hot path no entry covers, add a sizeable
benchmark for it **in its own commit first**, then run the gate with that
commit as `BASE` so both sides have it.

## Exploratory benchmarks

The other `Benchmark*` functions (grid sweeps, per-component micro
benches, `S3INV_LONG_BENCH=1` scale runs) are for investigation, not
gating. Run them directly, for example:

```sh
go test -run '^$' -bench BenchmarkGridQuery -benchtime 2s ./pkg/indexread/
S3INV_LONG_BENCH=1 go test -run '^$' -bench BenchmarkPipelineScale_E2E -benchtime 1x ./pkg/extsort/
```
