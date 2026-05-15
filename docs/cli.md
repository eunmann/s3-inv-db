# CLI Reference

`s3-inv-db <command> [flags]`

```
build    Build an index from an S3 inventory manifest
query    Query an existing index
```

The `--config <path>` flag (or `S3INV_CONFIG`) loads a JSON file
shared with the server. From the file the CLI honors `verbose`,
`pretty_logs`, and `price_table` only. Other server-side keys are
ignored. Explicit CLI flags always win over the file.

## `build`

```bash
s3-inv-db build --s3-manifest s3://bucket/inv/data/manifest.json --out ./my-index
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--s3-manifest` | yes |  | S3 URI to `manifest.json` |
| `--out` | yes |  | Output directory for index files |
| `--max-depth` |  | 0 (unlimited) | Maximum prefix depth to retain |
| `--segment-prefixes` |  | false | Dictionary-compress shared prefix segments |
| `--config` |  |  | JSON config file (see above) |
| `--verbose` |  | false | Debug-level logging |
| `--pretty-logs` |  | false | Human-friendly console output |

Memory and concurrency are no longer tunable per-flag. Worker counts
derive from `runtime.NumCPU()`; the process memory ceiling is
`GOMEMLIMIT` (env var, cgroup `memory.max`, or 60% of detected RAM —
whichever is smallest) installed at startup via
`runtime/debug.SetMemoryLimit`. Set `GOMEMLIMIT=4GiB` to cap a build,
or run under a constrained cgroup.

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

### Price-table JSON

```json
{
  "per_gb_month": {
    "STANDARD":     0.023,
    "STANDARD_IA":  0.0125,
    "GLACIER":      0.0036,
    "DEEP_ARCHIVE": 0.00099,
    "INTELLIGENT_TIERING_FREQUENT": 0.023,
    "INTELLIGENT_TIERING_FREQUENT_SMALL": 0.023
  },
  "monitoring_per_1000_objects": 0.0025,
  "put_per_1000_requests": 0.005,
  "standard_price_per_gb": 0.023
}
```

Prices are USD per GB-month for storage, USD per 1,000 objects for
monitoring, and USD per 1,000 requests for PUTs. Defaults match
us-east-1 as of 2026. See `pkg/pricing.DefaultUSEast1Prices` for the
seed values; tier semantics (small-IT bucket, IA min-billable size,
Glacier metadata overhead) are documented inline in `pkg/pricing`.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Invalid arguments, S3 failure, or build/query error |

`build` handles SIGINT / SIGTERM and removes its scratch temp files
before exit.
