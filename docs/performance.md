# Performance

This page is the canonical home for memory rules, build/query
costs, on-disk size, and benchmarks.

## Query

Lookup is constant-time in index size: hash the prefix (wyhash),
probe the MPHF, check the slot's fingerprint (a second wyhash with its
own seed), then read the core row. Tested up to ~10M
prefixes with consistent sub-microsecond warm-cache lookups.

| Operation | Complexity | Typical warm latency |
|-----------|------------|---------------------|
| `Lookup` | O(1) | sub-µs |
| `StatsForPrefix` | O(1) | sub-µs (~80 ns warm, 2.4M prefixes) |
| `TierBreakdown` | O(tiers) | sub-µs (~70 ns) |
| `DescendantsAtDepth` | O(log n + k) | µs–tens of µs (depends on k) |
| Browse page, by name | O(log n + page) | tens of µs, whatever the child count |
| Browse page, by size, objects or cost | O(c log c), c = children | ms at 200k children |

A name-sorted page reads only the page: the index stores siblings in S3
key order (each name with its trailing slash, so "2024-01/" lists
before "2024/", as in S3 itself), and that is the order the listing
shows. Sorting by a number reads one value per child.

The figures in parentheses come from `BenchmarkGateStatsForPrefix` and
`BenchmarkGateTierBreakdown` on the 1M-object gate fixture (2.37M
prefixes) on a warm mmap; `BenchmarkGridQuery` sweeps index sizes.
Cold-cache numbers are higher by roughly one page-fault per file
touched. Run the suite yourself:

```bash
go test -bench=BenchmarkGridQuery -benchtime=2s -run=^$ ./pkg/indexread/
```

Reads are lock-free; throughput scales linearly with cores up to
memory-bandwidth limits.

## Memory

The process memory ceiling is set at startup via
`runtime/debug.SetMemoryLimit`:

- If `GOMEMLIMIT` is set explicitly, it wins (capped only by the
  cgroup `memory.max`).
- Otherwise the limit is `min(0.85 × cgroup memory.max, 0.6 × detected RAM)`:
  the cgroup also charges page cache, mmap'd pages and stacks the Go
  limit doesn't count, so a soft limit at the hard cap invites the OOM
  killer.

This lets an operator opt into more than 60% of RAM on a dedicated
host while still being bounded by a tighter container cap.

The build aggregator spills when **any** of three thresholds fires:

1. The combined per-worker aggregator footprint exceeds `0.15 × GOMEMLIMIT`
   (or 512 MiB if no limit is configured), divided across N chunk workers.
2. Overall `HeapInuse` crosses 85% of the limit — a safety valve for
   when the per-worker estimate undercounts runtime reality.
3. A mid-chunk check every 50K rows — so a single very large chunk
   can't grow past the cap before its first spill opportunity.

After all inventory files are processed, a k-way merge combines run
files (zstd-compressed) into the final sorted stream, which the index
builder consumes directly. The final merge reads up to 64 runs at once.
Only past that do disk rounds run, merging just the smallest runs needed
in groups of up to 16, and each run is deleted as soon as it has been
merged. The merge decodes a few 4096-row batches ahead of the index
build on its own goroutine. Run-file buffers, the merge heap and the
index-build streaming arrays each take bounded space outside the
aggregator cap.

```bash
GOMEMLIMIT=4GiB s3-inv-db build ...   # cap at 4 GiB
s3-inv-db build --max-depth 5 ...     # limit prefix count
```

Worker counts and S3 concurrency derive from `runtime.NumCPU()`:
parser + download workers scale to `min(NumCPU, manifest_files)` (at
least 2), each
download fetches up to `min(max(NumCPU, 4), 16)` 16 MiB parts at once,
and GETs in flight across all downloads are capped at
`min(max(4 × NumCPU, 32), 256)`. The same binary scales from a laptop
to an ingest node without flags.

### Bottleneck checklist

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| Low CPU during build | S3 bandwidth limited | Run on a host with more vCPUs |
| Slow build, lots of temp files | Aggregator spilling under a tight `GOMEMLIMIT` | Raise the limit or run on a fatter host |
| Slow final build phase | Large MPHF build | Expected for >5M prefixes |
| First query slow | Cold page cache | The server prefaults loaded indexes up to 1/8 of its memory together; past that, and in the CLI, pages are read as queries touch them |
| Descendant queries slow | Large subtrees | Use `DescendantsAtDepthFiltered` with `MinCount` / `MinBytes` |

Query-time RAM is just the OS page cache plus iterator stack frames;
"high memory usage" on a warm process is page cache and is correct.

Index files are mapped with the kernel's default readahead. On NVMe a
fault that reads a little more costs less than more faults: on the
benchmark fixture, cold lookups were 33% faster than with
`MADV_RANDOM` on the MPHF slots, at the cost of ~20 KiB more page cache
per cold lookup, and `MADV_RANDOM` everywhere doubled cold latency
(#50). A server holding many indexes in little memory evicts sooner for
it.

The trade depends on how many queries share a cold period more than on
index size. `MADV_RANDOM` on the lookup files (MPHF slots, core and tier
stats) makes the first queries after an eviction cheaper, but readahead
warms the cache faster for everything after them. Measured from 1M to
8M objects (#77):

| queries per cold period | 2M objects | 8M objects |
|---|---|---|
| 100 | hint −23% | hint −34% |
| 1,000 | hint +8% | hint −26% |
| 10,000 | hint +124% | hint +26% |

A server answers many queries per load, so the default stays. Re-measure
before adding a hint for a workload of a few queries per cold period on
slow storage.

When the server loads an index (a fresh build or a restart), it reads
the files its manifest lists into the page cache first
(`format.Prefault`, `MADV_POPULATE_READ`) if the index fits in what is
left of 1/8 of the server's memory (host RAM, or the cgroup
`memory.max` when lower) after the loaded indexes it already
prefaulted. So a restart reads at most that 1/8 before it serves, and
one index's prefault doesn't push the others out of the page cache.
Opening an evicted 2.4M-prefix index plus its first 1000 lookups took
36 ms instead of 77 ms. The asynchronous `MADV_WILLNEED` measured no
gain. A prefault that fails only logs a warning; the index opens
without it. CLI commands open without it: a one-off query shouldn't
read a whole index (#55).

## Index size

Every per-prefix file has a fixed stride per index, except the
multi-tier slots, which only prefixes with two or more tiers store, so
total size scales linearly with prefix count. Strides adapt to the
index: `object_count`, `total_bytes` and subtree size take the core
widths that make the core files smallest, with the few rows that don't
fit (the root and top levels) patched exactly in `core_patch.*`; every
other core field, tier slot and offset or position array is stored at
the narrowest byte width its largest value needs. Tier stats store only what a prefix's totals and tier mask don't give,
in whichever of two layouts is smaller for the index. Layouts:
[index-format.md](index-format.md).

Measured on the S3-realistic 1M-object fixture
(`benchutil.S3RealisticConfig(1_000_000)`, built through
`extsort.NewAggregator` → `Drain` → `SortPrefixRows` → `IndexBuilder` the way
`pkg/indexread`'s `BenchmarkGate*` fixture is): 2,374,251 prefixes,
max depth 7, 11 of 13 tiers present, 95% of prefixes holding one tier.

| Component | Files | B/prefix |
|---|---|---|
| Core stats | `core_stats.bin` (10-byte rows: 1 + 5 + 1 + 1 + 2) + `core_patch.bin` / `core_patch.rows` (0.04% of rows patched) | 10.2 |
| Tier stats | `tier_stats/tier_multi.*` (multi layout) | 1.4 |
| MPHF | `mph_slots.bin` (8 B fingerprint + 3 B position) + `mph.bin` (BBHash, ~3.3 bits) | 11.4 |
| Prefix strings | `prefix_tree.bin` (2 B segment ID + 3 B parent distance) + segment blob | 5.1 |
| Depth index | `depth_positions.u64` (3 B) + `depth_offsets.u64` | 3.0 |
| **Total** | including `manifest.json` and `tiers.json` | **31.0** |

The fingerprint is 8 B in every index; the position takes the bytes
the largest position needs. Tier
stats depend on how many tiers each prefix holds: nothing for an index
with one tier, up to (T − 1) slots per prefix when every prefix holds
all T tiers. The other components grow by a byte per field as counts,
sizes and prefix totals cross powers of 256. `s3-inv-db stats --profile`
shows those distributions for an index.

## Server: sizing the disk budget

When auto-load is enabled, `--max-index-disk` caps the total bytes
the server will hold for materialised indexes.
`BenchmarkLoadDiskPeak` in `pkg/extsort` measures the final index
size and scratch peak for synthetic loads:

| Synthetic objects | Final index bytes | Run-file scratch |
|---|---|---|
| 10 K | ~2.1 MB | ~0.6 MB |
| 100 K | ~18.6 MB | ~5.6 MB |
| 500 K | ~86.7 MB | ~26 MB |

The scratch column is the compressed run files only, sampled before
the merge. The index build also keeps temporary files in the same
temp directory until it finishes (while it writes slots, the MPHF
builder alone holds three 8-byte arrays per prefix plus a slot-owner
map of 4 bytes per prefix, 8 past 2^32 prefixes), which the table does
not count.

These scale near-linearly with prefix count, not object count. The
planner applies a fixed `0.30` multiplier to a manifest's compressed
CSV total when estimating final index bytes — conservative, but
measure your own corpus if disk budgets are tight. A build keeps its
scratch (downloaded chunks, run files, MPHF scratch) in a hidden
directory beside its output (`.<out>.scratch-*`), not in
`os.TempDir()`: on hosts where `/tmp` is tmpfs that would be RAM the Go
memory limit can't see. For the server that is its cache volume; keep
it sized to at least 2× the largest manifest-compressed-size you
intend to load, on top of the indexes. A killed build's scratch is
reclaimed by the next build of that output and by the server's startup
reconcile.

## Running benchmarks

Ingestion and query changes are gated by an A/B comparison against the
base commit on the same machine — `make bench-ingest` / `make
bench-query`; see [bench/README.md](../bench/README.md) for when to run
which. The commands below are for
exploration.

```bash
go test -bench=. -benchmem ./pkg/indexread/                          # all indexread benches
go test -bench=BenchmarkMPHFBuild -benchmem ./pkg/format/             # MPHF build
go test -bench=BenchmarkMPHFQuery -benchmem ./pkg/format/             # MPHF query
go test -bench=BenchmarkBBHash -benchmem ./pkg/format/               # BBHash build + find alone
S3INV_LONG_BENCH=1 go test -bench=. -benchmem ./pkg/indexread/        # large dataset

# Memory profile
go test -bench=BenchmarkGridQuery -benchmem -memprofile=mem.out ./pkg/indexread/
go tool pprof mem.out
```
