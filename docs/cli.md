# CLI Reference

`s3-inv-db <command> [flags]`

```
build         Build an index from an S3 inventory manifest
query         Look up a single prefix in an existing index
top           Top-N descendants by bytes or count
browse        List children of a prefix at a relative depth
compare       Diff two index directories at a prefix
verify        Validate index integrity (manifest + MPHF roundtrip)
stats         Inspect index metadata + per-file sizes
config-check  Validate a JSON config file and print set fields
```

`s3-inv-db --version` prints the version, commit and commit date on
one line and exits; `s3-inv-db-server` and `s3-inv-db-seeder` accept
`--version` too.

Every subcommand except `build` accepts `--output text|json` (default
`text`) — pass `--output json` to emit a single JSON object on stdout
for scripting.

The `--config <path>` flag (every subcommand except `verify` and
`stats`) loads a JSON file shared with the server.
From the file the CLI honors `verbose`, `pretty_logs`, `price_table`,
and `build_event_log` (the last is used only by `build`). Other
server-side keys are ignored. Explicit CLI flags always win over the
file.

## `build`

```bash
s3-inv-db build --s3-manifest s3://bucket/inv/data/manifest.json --out ./my-index
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--s3-manifest` | yes |  | S3 URI to `manifest.json` |
| `--out` | yes |  | Output directory for the index: missing, empty, or a previous index (which is replaced). Or `s3://bucket/prefix/` to publish it for a server's `index_source` (see [operations](operations.md#builds-outside-the-server)) |
| `--work-dir` |  | `.` | With `--out s3://...`: the local directory the index is built in before it's uploaded, scratch included. Not `/tmp`, which may be RAM |
| `--max-depth` |  | 0 (unlimited) | Maximum prefix depth to retain |
| `--allow-malformed-rows` |  | false | Build even when inventory rows can't be read exactly (a short row, an empty or null key, a size that doesn't parse or is null, a key that isn't valid URL encoding). They're skipped or indexed with size 0 and logged, so totals differ from the inventory. Without it the build fails and names the chunk and counts. A chunk whose downloaded size differs from the manifest always fails. CLI only: builds the server starts have no opt-out and always fail on such rows. |
| `--event-log` |  |  | Append build events as JSONL to this path (overrides `build_event_log`) |
| `--config` |  |  | JSON config file (see above) |
| `--verbose` |  | false | Debug-level logging |
| `--pretty-logs` |  | false | Human-friendly console output |

The index is built in a hidden sibling of `--out` and renamed into
place only when complete. A failed or cancelled build leaves `--out`
exactly as it was and logs its summary; a successful one also writes
that summary to `build.json` in the index. A directory holding anything
other than an index is refused before any work starts. Hidden build
directories left beside `--out` by a killed build are removed by the
next build into the same `--out`. Concurrent builds into the same
`--out` are not supported: a starting build removes every hidden build
directory beside `--out`, including one a still-running build is
writing, and that build then fails.

Worker counts and S3 part concurrency derive from `runtime.NumCPU()`;
the process memory ceiling is `GOMEMLIMIT`. Both are intentionally
flagless: see [how it works](how-it-works.md#building) and
[operations](operations.md#memory).

AWS credentials use the standard SDK chain: env vars → shared
credentials file → IAM role. Required permissions: `s3:GetObject` on
the inventory files and `s3:ListBucket` on the inventory bucket;
publishing also needs `s3:PutObject` and `s3:DeleteObject` on the
index prefix.

## `query`

```bash
s3-inv-db query --index ./my-index --prefix "data/2024/"
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--index` | yes |  | Index directory (built by `build`) |
| `--prefix` | yes |  | Prefix to look up; must be non-empty, so the root `""` cannot be queried |
| `--show-tiers` |  | false | Print per-tier object count / bytes |
| `--estimate-cost` |  | false | Add monthly cost per tier and total |
| `--price-table` |  | us-east-1 built-in | Path to JSON price table |
| `--config` |  |  | JSON config file (see above) |
| `--verbose` |  | false | Debug-level logging |
| `--pretty-logs` |  | false | Human-friendly console output |

`--price-table` points at a JSON file with per-GB-month storage rates,
per-1,000-object monitoring rates, and per-1,000-request PUT rates.
The schema, defaults (us-east-1, 2025), and tier semantics (IT-Frequent-Small
bucket, IA minimum-billable size, Glacier metadata overhead) live in
`pkg/pricing` — see `DefaultUSEast1Prices` for the seed values and the
inline package docs for the field-by-field rules.

## `top`

```bash
s3-inv-db top --index ./my-index --parent "data/" --depth 1 --limit 10 --by bytes
```

Ranks descendants at the given relative depth under `--parent` by
total bytes or object count. Defaults: `--parent ""` (root), `--depth
1`, `--limit 25`, `--by bytes`. `--min-count` / `--min-bytes` apply a
filter before ranking.

## `browse`

```bash
s3-inv-db browse --index ./my-index --prefix "data/2024/" --depth 1
```

Lists the children at `--depth` (default 1) below `--prefix` (default
`""`, the root) with per-child object count and total bytes. A prefix
not in the index is an error. Equivalent to the HTTP `/api/browse` endpoint
without the server.

## `compare`

```bash
s3-inv-db compare --from ./old-index --to ./new-index --prefix "" --depth 1
```

Per-prefix diff of two indexes over the descendants at `--depth`
(default 1) below `--prefix` (default `""`, the root). A prefix
missing from one index contributes no rows from that side. Each row reports
the from / to counts and bytes, the deltas, and a status of `added`,
`removed`, `changed`, or `unchanged`.
A prefix present in both is `changed` when its object count, bytes or
per-tier breakdown differ, so a row whose objects only moved between
storage classes is `changed` with zero count and byte deltas.

## `verify`

```bash
s3-inv-db verify --index ./my-index
```

Runs `format.VerifyManifest` against the index directory and an MPHF
roundtrip (`Lookup(PrefixString(pos)) == pos`) for every prefix. Use
`--sample N` to limit the roundtrip check to the first N positions
for very large indexes (0, the default, checks every prefix). Exits
non-zero on any failure.

## `stats`

```bash
s3-inv-db stats --index ./my-index
```

Prints node count, max depth, tier-data availability, total on-disk
size, and per-file sizes (largest first). Counts and sizes come from
the manifest; tier-data availability opens the index.

`--profile` also scans every prefix and reports the shape of the data,
independent of how the index stores it:

- prefixes per depth, the number of leaves;
- how many prefixes hold 1, 2, … tiers, and how many distinct tier
  combinations occur;
- per value (object count, total bytes, child start, distance to the
  parent, children, per-tier count and bytes), the bits it needs at the
  median, p99, p99.9 and maximum (the JSON form has the full histogram);
- the segment dictionary's size.

The scan reads the whole index. Use it to see what a real inventory
looks like before changing a layout (see `bench/README.md`).

## `config-check`

```bash
s3-inv-db config-check --config ./config.json
```

Validates the JSON config (rejects unknown fields, enforces inventory
entry shape, checks `trusted_proxies`) and prints every set field.
Empty `--config` succeeds: an empty config is valid. It does not parse
sizes or durations, or catch `auto_load` without `max_index_disk`;
those fail when the server starts. See
[configuration](configuration.md) for every key.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Invalid arguments, S3 failure, or build/query error |

`build` handles SIGINT / SIGTERM and removes its scratch temp files
before exit.
