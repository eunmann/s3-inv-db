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
tooling — and review fixes that stay outside those two areas. CI runs
only the weekly scale suite (below).

Within those two areas, run the gate only when the change can
plausibly move hot-path time, memory or allocations. A change that
cannot (a correctness fix that adds a check, a cleanup call, error
handling, validation, a log line, a rename) skips the gate and says so
in its PR's **Performance** section in one line, naming why. When
unsure, run it.

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
| `peak_heap_B` (sampled, so noisier), `retained_heap_B`, `gc_scan_heap_B` | `BENCH_HEAP_PCT` (default 15%) |

Exit status 1 is a regression. Exit status 2 means nothing was compared:
a build or benchmark failed, a suite entry matched no benchmark on one
side, or the S3 server did not come up. The script names the side and
entry and prints the tail of the failing run.

Results land in `bench/ab/<timestamp>/` (gitignored): raw `base.txt` /
`head.txt`, each side's full output in `base.log` / `head.log`,
`report.txt` and `report.csv`. Entries tagged `integration` need a local
S3 server; the script starts one in Docker unless `AWS_ENDPOINT_URL_S3`
is already set. The pipeline benchmarks seed each inventory fixture
(shape, format, size, chunk count) once per server and side, into a `fixture-*`
bucket, and every later run of that side reuses it, so rounds don't
re-upload. The script sets `S3INV_FIXTURE_TAG` to `base-<sha>` or `head`
for each side's runs, so head always builds from its own seeder's
inventory, even when the change under test is to the seeder. The bucket
name also carries a fingerprint of the seeder's output (a hash of a
small sample inventory), so a seeder that writes different bytes seeds
a new fixture rather than reusing an old one. A server the script
starts is thrown away with the run; against your own server, delete
stale `fixture-*` buckets to reclaim the space.

A regression is merged only with an explicit justification in the
commit message (CLAUDE.md, Performance & Benchmarks).

### Noise floor

An A/A run (`make bench-ingest BASE=HEAD` on a clean tree: the same
commit on both sides) shows what the gate reports when nothing changed.
Calibrated in #142 at 12 pinned cores, 8 rounds:

| Entry | Metric | A/A spread | Note |
|---|---|---|---|
| Pipeline (all shapes), PipelineSlowS3 | `sec/op` | ±1–4% | |
| Pipeline (all shapes), PipelineSlowS3 | `peak_heap_B` | ±3–16% | sampled every 5 ms |
| FinalMerge (all) | `sec/op` | ±1–5% | |
| FinalMerge (all) | `peak_heap_B` | ±4–16% | where GC cycles land in the merge |
| MPHFBuild | `sec/op` | ±1% at `10x` | ±12–15% at `1x`, hence `10x` |
| Gate* (indexread, handlers) | `sec/op` | ±0–3% | |
| OpenManySegments | `sec/op` | ±5–12% | one `Open` per op; its gate is `retained_heap_B` |
| everything | `B/op`, `allocs/op`, `disk_B` | ±0–2% | |

What this means for reading a report:

- **Significant is not the same as real.** Across the four A/A runs, 2
  of ~60 time and heap comparisons came out significant (`+3.1%`
  peak heap, `+2.5%` time), both under the thresholds. A gate on #112
  saw FinalMerge time `+1.6%` to `+3.2%` (p<0.05) on code the change
  never runs. A sub-threshold move on an entry the change can't reach is
  drift; on an entry it does reach, check that `B/op`/`allocs/op` or the
  entry's own counters move with it before claiming it.
- The thresholds sit above every A/A spread except the heap peaks,
  which are why `BENCH_HEAP_PCT` is 15%. A heap move under ~15% on
  FinalMerge or the pipeline is not evidence on its own; an A/B of the
  binaries over more rounds is.

**Cooldown.** `BENCH_COOLDOWN` (seconds slept before every run, default
0) did not reduce noise on the calibration machine: ingest spreads at
10 s and 2 s were the same, query at 3 s and 0 s too, and the cooldown
was most of the gate's wall time (ingest 1100 s at 10 s, 592 s at 2 s;
query 731 s at 3 s, 583 s at 0 s). Leave it at 0. It exists for
machines that throttle: on a laptop, sustained back-to-back runs can
trip power or thermal limits, and the lower clock can land on one side
only, which interleaving doesn't cancel. In #61, Go 1.27's runs dropped
from ~3.5 to ~3.1 GHz from round 9 onward while the interleaved 1.26
runs kept their clock, a +3.4% `sec/op` "regression" with identical
cycle and instruction counts. When `sec/op` moves with no matching
change in `B/op` or `allocs/op` on such a machine, rerun with
`BENCH_COOLDOWN=20` before believing it. Rerun the A/A whenever the
machine or the suites change.

A Go toolchain upgrade changes generated code everywhere, so it runs
both gates with the old toolchain on the base side:
`BASE_GOTOOLCHAIN=go1.26.10 make bench-query BASE=main`. Without it,
`GOTOOLCHAIN=auto` builds both sides with the newest local toolchain.

## The suites

Every entry does milliseconds to seconds of work per op on fixed,
seeded inputs, so repeated rounds agree.

Ingestion (`bench/suite/ingest.txt`):

- `internal/loader` `BenchmarkPipeline` at 1M objects (realistic,
  deep-pyramid and realistic_sorted shapes, and realistic from Parquet;
  see the fixtures below):
  the real `Pipeline.Run` end to end through the local S3 server.
  Seconds per op; also reports `input_B` (the data files a build
  downloads), `peak_heap_B`, `disk_B`, `runs` (sorted runs the
  merge read, in memory or on disk) and `run_files` (runs written to
  disk). Per op, over every thread: `cpu_s` and `sys_s` (CPU seconds),
  `cores` (CPU over wall time: a change that saves CPU on a core-rich
  machine shows here before it shows in `sec/op`), `minflt`/`majflt`,
  and `read_B`/`write_B` (storage I/O; writes net of scratch deleted
  before writeback). Reported, not gated.
- `internal/loader` `BenchmarkPipelineSlowS3`: the same build against
  S3 behind a slower link (20 ms to first byte, 8 MiB/s per GET), four
  chunks per worker: the time a build waits on S3.
- `pkg/format` `BenchmarkMPHFBuild` at 1M prefixes: the MPHF finalize
  step on its own, with `peak_heap_B`.
- `pkg/extsort` `BenchmarkFinalMerge`: 48 or 128 spilled runs merged
  under a 256 MiB or 1 GiB `GOMEMLIMIT`, heavy and light key overlap;
  reports `peak_heap_B`, `rounds` and `round_bytes` (fan-in sizing).
- `pkg/tiers` `BenchmarkFromS3`: storage class and access tier to tier
  ID, once per row, for canonical, Intelligent-Tiering and lowercase
  input; reports `ns/row`.

The pipeline fixtures model an AWS CSV inventory: gzipped data files
with no header, every field quoted, keys URL-encoded, and a
random-looking ETag (multipart form above 8 MiB) and LastModifiedDate
per object, so the gzip ratio and parse work resemble a real file (1M
realistic objects gzip to about 51 MiB). Each is split into up to four
data files per ingest worker, at least 10,000 objects per file, so
every worker has chunks queued, as with a real inventory's many files. Realistic and deep_pyramid list keys in
random order, the case to expect: S3 Inventory guarantees no order.
Realistic_sorted lists them in key order across the files, the best
case for the build's in-order paths. `shape=realistic/format=parquet`
is the realistic objects as Snappy-compressed Parquet in AWS's column
layout (raw keys, the same ETags and dates; about 66 MiB for 1M), so
a build from each format can be compared on the same objects. An A/B
across a seeder change
builds different fixtures on each side, so its pipeline numbers are a
new baseline, not a regression signal.

Data structures / query (`bench/suite/query.txt`):

- `pkg/indexread` `BenchmarkGate*`: 100k lookups, prefix
  reconstructions or tier breakdowns, or 1k children + top-10 queries
  per op, over one 1M-object multi-tier index.
- `pkg/indexread` `BenchmarkOpenManySegments`: `Open` of an index with
  500k distinct path segments; reports what the open index holds on the
  Go heap after a GC (`retained_heap_B`) and how much of the heap the
  collector scans (`gc_scan_heap_B`).
- `internal/handlers` `BenchmarkGate*`: one browse or compare API
  request over a prefix with 200k children, sorted by name, size or
  cost (cost prices every child's tier mix).

Scale (`bench/suite/scale.txt`, `make bench-scale BASE=<ref>`,
`SCALE_ROUNDS=4` by default, `S3INV_LONG_BENCH=1` set for you):

- `internal/loader` `BenchmarkPipeline` at 10M objects (realistic,
  realistic_sorted, and realistic from Parquet).
- `pkg/format` `BenchmarkMPHFBuild` at 5M prefixes.
- `pkg/indexread` `BenchmarkBrowse_Matrix` warm, over a 10M-object index.

It is too slow for every PR. Run it on demand for a decision the 1M
fixtures can't settle: spills and merge rounds, peak heap, index size
or query cost at scale. CI runs it weekly
([`bench-scale.yml`](../.github/workflows/bench-scale.yml)), comparing
main with main of a week earlier on one runner, and keeps the reports
as an artifact. A regression fails that run but blocks no merge.

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

## Profile-guided optimization

`go build` compiles `cmd/s3-inv-db-server` and `cmd/s3-inv-db` with the
`default.pgo` beside their `main.go`: a CPU profile of the query gate
benchmarks, which steers inlining along the read paths. Benchmarks run
through `go test` don't use it, so the gates compare the same code they
always did. Regenerate the profiles with `make pgo` after a large change
to the read path. The build path is left out: a profile merged from the
query gates and the pipeline builds measured mixed (sorted builds -3.3%,
random-order +1.7%, query gates neutral; issue #186). The build is
compute- and front-end-bound, not memory-bound.
