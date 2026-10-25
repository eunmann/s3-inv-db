# Benchmarks

## When to run

Run the A/B gate **only** when a change touches one of two areas, and
run **only that area's suite** — a full run takes a long time:

| Change touches | Run |
|---|---|
| Ingestion: inventory parsing (`pkg/s3inventory`), aggregation, spill/merge and index build (`pkg/extsort`, `pkg/format` writers) | `make bench-ingest BASE=main` |
| Index data structures or query code: `pkg/format` readers, `pkg/indexread`, browse/compare/stats paths (`internal/inventory`, `internal/handlers`) | `make bench-query BASE=main` |

Everything else is **not** benchmarked: dependency bumps, server,
catalog, auto-load, jobs, security and lifecycle code, templates, docs,
tooling — and review fixes that stay outside those two areas. CI never
runs benchmarks.

Every PR that ran the gate reports the result in its description under
**Performance**: per suite entry, before → after for time, memory and
allocations per op (and peak heap / disk for the pipeline), so readers
know what to expect. A PR that did not need the gate says so in one
line.

## The A/B gate

```sh
make bench-ingest BASE=main        # or bench-query; ROUNDS=8 by default
make bench-ab SUITE=<file> BASE=main   # a custom suite
```

[`scripts/bench-ab.sh`](../scripts/bench-ab.sh) builds the test binaries
for `BASE` (in a temporary worktree) and for the working tree, then runs
every entry of the suite file (`bench/suite/ingest.txt` or
`bench/suite/query.txt`) `ROUNDS` times, alternating which
side runs first so machine drift hits both sides equally. Base and head
always run on the same machine in the same session, so no stored
baseline can go stale with hardware. `benchstat` compares them, and the
script fails on a statistically significant (p < 0.05) regression beyond:

| Metric | Threshold |
|---|---|
| `sec/op` | `BENCH_TIME_PCT` (default 5%) |
| `B/op`, `allocs/op`, `disk_B` | `BENCH_MEM_PCT` (default 5%) |
| `peak_heap_B` (sampled, so noisier) | `BENCH_HEAP_PCT` (default 15%) |

Exit status 1 is a regression. Exit status 2 means nothing was compared:
a build or benchmark failed, a suite entry matched no benchmark on one
side, or the S3 server did not come up. The script names the side and
entry and prints the tail of the failing run.

Results land in `bench/ab/<timestamp>/` (gitignored): raw `base.txt` /
`head.txt`, each side's full output in `base.log` / `head.log`,
`report.txt` and `report.csv`. Entries tagged `integration` need a local
S3 server; the script starts one in Docker unless `AWS_ENDPOINT_URL_S3`
is already set.

A regression is merged only with an explicit justification in the
commit message (CLAUDE.md, Performance & Benchmarks).

## The suites

Every entry does milliseconds to seconds of work per op on fixed,
seeded inputs, so repeated rounds agree.

Ingestion (`bench/suite/ingest.txt`):

- `internal/loader` `BenchmarkPipeline` at 1M objects (realistic and
  deep-pyramid shapes): the real `Pipeline.Run` end to end through the
  local S3 server. Seconds per op; also reports `peak_heap_B` and
  `disk_B`.
- `pkg/format` `BenchmarkMPHFBuild` at 1M prefixes: the MPHF finalize
  step on its own, with `peak_heap_B`.

Data structures / query (`bench/suite/query.txt`):

- `pkg/indexread` `BenchmarkGate*`: 100k lookups, prefix
  reconstructions or tier breakdowns, or 1k children + top-10 queries
  per op, over one 1M-object multi-tier index.
- `internal/handlers` `BenchmarkGate*`: one browse or compare API
  request over a prefix with 200k children.

When a change touches a hot path no entry covers, add a sizeable
benchmark for it **in its own commit first**, add it to the matching
suite file, then run the gate with that commit as `BASE` so both sides
have it.

## Cache, page and storage costs

The gate suites time warm queries. Layout changes (index format, field
widths, madvise hints) also change how many cache lines, pages and
storage reads a query costs, which warm ns/op hides. Measure those with:

- **`s3-inv-db stats --index <dir> --profile`**: the data's shape (tiers
  per prefix, value bit widths, tree shape). Decide layouts from these
  numbers, ideally on a real index, never from what the generator emits.
- **`pkg/indexread` `BenchmarkCache*`** (lookup, tier breakdown, prefix
  string, a 50-row browse page), each with uniform and Zipf-skewed keys,
  `state=warm` and `state=cold` (index closed, evicted from the page
  cache and reopened before every op; Linux keeps pages a mapping holds,
  so the drop has to come after the close). `S3INV_LONG_BENCH=1` adds a ~10M-prefix
  index. Per query they report:

  | Metric | Source | Meaning |
  |---|---|---|
  | `ns/q` | timer | latency |
  | `minflt/q`, `majflt/q` | `getrusage` | pages mapped from the page cache / read from storage |
  | `ioKiB/q` | `/proc/self/io` `read_bytes` | storage read, readahead included |
  | `<file>KiB/q` (cold) | `/proc/self/smaps` `Rss` | how much of each structure the queries mapped in, beyond what `Open` mapped |
  | `cycles/q`, `instr/q`, `IPC`, `L1Dmiss/q`, `LLCmiss/q`, `dTLBmiss/q` | `perf_event_open` | CPU cache and TLB behaviour |

  Run them pinned to the performance cores and A/B them like any suite:

  ```sh
  go test -c -o /tmp/indexread.test ./pkg/indexread
  taskset -c 0-11 /tmp/indexread.test -test.run '^$' -test.bench '^BenchmarkCache' -test.benchtime 20x -test.count 6 2>/dev/null
  ```

The hardware counters need `kernel.perf_event_paranoid` ≤ 2 (`sudo
sysctl kernel.perf_event_paranoid=2`; Ubuntu defaults to 4). Without
it those metrics are left out and the rest still run. Go has no
built-in access to these counters: `pprof` and `go tool trace` show CPU
time, allocations and scheduling, but no cache, TLB or page-fault
events. Linux's `perf` is the standard tool, and the in-process
counters open the same `perf_event_open` events. `perf stat` can scope
counting to a region (`--delay=-1 --control fd:...` starts with the
counters disabled and the program enables them), but it reports one
total per run. The in-process counters count the benchmark's own thread
around each sub-benchmark's measured loop and report per query through
`b.ReportMetric`, so `benchstat` compares them entry by entry. `perf
stat -e <event>` on the test binary with one `-test.bench` entry
cross-checks them, counting the fixture build and setup too. For where
in the code the misses happen, use `perf record -e <event>` or `perf
mem` on the test binary.

## Exploratory benchmarks

The other `Benchmark*` functions (grid sweeps, per-component micro
benches, `S3INV_LONG_BENCH=1` scale runs) are for investigation, not
gating. Run them directly, for example:

```sh
go test -run '^$' -bench BenchmarkGridQuery -benchtime 2s ./pkg/indexread/
S3INV_LONG_BENCH=1 go test -run '^$' -bench BenchmarkPipelineScale_E2E -benchtime 1x ./pkg/extsort/
```
