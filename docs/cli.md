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

Every subcommand accepts `--output text|json` (default `text`) — pass
`--output json` to emit a single JSON object on stdout for scripting.

The `--config <path>` flag loads a JSON file shared with the server.
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
| `--out` | yes |  | Output directory for index files |
| `--max-depth` |  | 0 (unlimited) | Maximum prefix depth to retain |
| `--config` |  |  | JSON config file (see above) |
| `--verbose` |  | false | Debug-level logging |
| `--pretty-logs` |  | false | Human-friendly console output |

Worker counts and S3 part concurrency derive from `runtime.NumCPU()`;
the process memory ceiling is `GOMEMLIMIT`. Both are intentionally
flagless — see [performance.md#memory](performance.md#memory) for the
exact resolution rule.

AWS credentials use the standard SDK chain: env vars → shared
credentials file → IAM role. Required permissions: `s3:GetObject` on
the inventory files and `s3:ListBucket` on the inventory bucket.

## `query`

```bash
s3-inv-db query --index ./my-index --prefix "data/2024/"
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--index` | yes |  | Index directory (built by `build`) |
| `--prefix` | yes |  | Prefix to look up (use `""` for root) |
| `--show-tiers` |  | false | Print per-tier object count / bytes |
| `--estimate-cost` |  | false | Add monthly cost per tier and total |
| `--price-table` |  | us-east-1 built-in | Path to JSON price table |
| `--config` |  |  | JSON config file (see above) |
| `--verbose` |  | false | Debug-level logging |
| `--pretty-logs` |  | false | Human-friendly console output |

`--price-table` points at a JSON file with per-GB-month storage rates,
per-1,000-object monitoring rates, and per-1,000-request PUT rates.
The schema, defaults (us-east-1, 2026), and tier semantics (IT-Frequent-Small
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

Lists the children at `--depth` below `--prefix` with per-child object
count and total bytes. Equivalent to the HTTP `/api/browse` endpoint
without the server.

## `compare`

```bash
s3-inv-db compare --from ./old-index --to ./new-index --prefix ""
```

Per-prefix diff of two indexes at the given parent. Each row reports
the from / to counts and bytes, the deltas, and a status of `added`,
`removed`, `changed`, or `unchanged`.

## `verify`

```bash
s3-inv-db verify --index ./my-index
```

Runs `format.VerifyManifest` against the index directory and an MPHF
roundtrip (`Lookup(PrefixString(pos)) == pos`) for every prefix. Use
`--sample N` to limit the roundtrip check to the first N positions
for very large indexes. Exits non-zero on any failure.

## `stats`

```bash
s3-inv-db stats --index ./my-index
```

Prints node count, max depth, tier-data availability, total on-disk
size, and per-file sizes (largest first). Reads the manifest, not the
mmap.

## `config-check`

```bash
s3-inv-db config-check --config ./config.json
```

Validates the JSON config (rejects unknown fields, enforces inventory
entry shape) and prints every set field. Empty `--config` succeeds —
an empty config is valid.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Invalid arguments, S3 failure, or build/query error |

`build` handles SIGINT / SIGTERM and removes its scratch temp files
before exit.
