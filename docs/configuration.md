# Configuration

Every config key, server flag and environment variable. Per-subcommand
CLI flags are in the [CLI reference](cli.md); the authoritative list is
the `Config` struct in
[`internal/appconfig/appconfig.go`](../internal/appconfig/appconfig.go).

`s3-inv-db-server` and `s3-inv-db` share one JSON file, passed with
`--config`. An explicit flag wins (even when it equals the default),
then the file, then the default. Unknown keys are an error. Each binary
ignores keys it doesn't use: the CLI reads only `verbose`,
`pretty_logs`, `price_table` and `build_event_log`. Check a file with
`s3-inv-db config-check --config <path>` (it doesn't parse sizes or
durations; those fail at server start).

```json
{
  "addr": ":8080",
  "s3_source": "s3://my-bucket/inventory-data/",
  "cache_dir": "/var/cache/s3inv",
  "auto_load": true,
  "max_index_disk": "200GB",
  "max_concurrent_jobs": 2,
  "auto_load_poll_interval": "15m",
  "discovery_refresh_interval": "1m",
  "level_cache": "64MiB",
  "trusted_proxies": ["10.0.0.0/8"],
  "inventories": [
    {"source": "prod-bucket", "name": "daily-inventory", "auto_load": true, "retention_count": 5}
  ]
}
```

## Keys

| Key | Flag | Default | What it does |
|---|---|---|---|
| `addr` | `--addr` | `:8080` | Listen address. |
| `s3_source` | `--s3-source` | empty | `s3://bucket/prefix/` where AWS writes inventories. Empty turns discovery off. |
| `cache_dir` | `--cache-dir` | `/var/cache/s3inv` | Indexes, build scratch and `state.db`. Required with `s3_source`. |
| `max_index_disk` | `--max-index-disk` | none | Disk cap for loaded indexes, manual and automatic; a fifth is headroom. Required with `auto_load`. Without it, nothing is evicted. See [operations](operations.md#disk-budget). |
| `auto_load` | `--auto-load` | `false` | Build the newest runs of auto-load configurations automatically. |
| `auto_load_poll_interval` | `--auto-load-poll-interval` | `15m` | How often auto-load checks for new runs. |
| `auto_load_retention_default` | `--auto-load-retention-default` | `2` | Retention for a configuration with no stored row. Configurations from `inventories[]` or the UI store their own (2 if unset); see #149. |
| `discovery_refresh_interval` | `--discovery-refresh-interval` | `1m` | How often the run list is refreshed. |
| `max_concurrent_jobs` | `--max-concurrent-jobs` | `1` | Builds that run at once. |
| `query_batch_max` | `--query-batch-max` | `1000` | Most prefixes in one batch stats request. |
| `level_cache` | `--level-cache` | `64MiB` | Memory for the full order of levels with 4096+ children, so paging through them reads only the page. `0` turns it off. |
| `trusted_proxies` | `--trusted-proxies` | none | CIDRs or IPs whose `X-Forwarded-For` is believed; otherwise the client IP is the TCP peer. |
| `price_table` | `--price-table` | built-in us-east-1 | JSON price table for cost estimates (server and `query`). |
| `build_event_log` | `--event-log` (`build`) | off | `s3-inv-db build` appends its events here as JSON lines. |
| `verbose` | `--verbose` | `false` | Debug-level logs. |
| `pretty_logs` | `--pretty-logs` | `false` | Human-readable logs instead of JSON lines. |

Sizes are strings like `"50GB"`, `"1.5GiB"` or `"64 MiB"` (KB…TB are
powers of 1000, KiB…TiB of 1024). Durations use Go's form: `"90s"`,
`"15m"`, `"1h30m"`.

## `inventories[]`

Per-configuration settings, written to the state database at every
startup. Pins are set in the UI, not here.

| Field | Default | What it does |
|---|---|---|
| `source` | required | Source bucket (the folder under `s3_source`). |
| `name` | required | Inventory configuration ID. |
| `auto_load` | `false` | Keep this configuration's newest runs built. Written on every start, so it overrides a UI toggle (#149). |
| `retention_count` | stored value, or 2 | How many newest runs to keep loaded. |

## Environment variables

| Variable | What it does |
|---|---|
| `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_PROFILE`, … | The AWS SDK's standard settings and credential chain. Set `AWS_REGION` to the inventory bucket's region. |
| `AWS_ENDPOINT_URL_S3` | Use a non-AWS S3 server (MinIO, versitygw), with path-style addressing. |
| `GOMEMLIMIT` | Memory limit. Unset: `min(0.85 × cgroup memory.max, 0.6 × RAM)`; see [operations](operations.md#memory). |
| `GOGC` | Left alone when set; otherwise 50 when the limit is 4 GiB or less. |
| `S3INV_MEM_DEBUG=1` | Log build memory stats every 5 s; with `S3INV_MEM_PPROF=1`, also serve pprof on `:6060`. |

Builds need `s3:GetObject` on the manifest and data files; discovery
also needs `s3:ListBucket` on the destination bucket.
