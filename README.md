# s3-inv-db

Turns an [S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html)
report into a memory-mapped, prefix-aggregated index, then answers
"how much data is under `prefix/`?" in microseconds — through a JSON +
HTMX HTTP server, a CLI, and a Go library.

```
┌──────────┐    ┌────────────┐    ┌──────────┐    ┌─────────────────┐
│ S3       │ →  │ build      │ →  │ mmap     │ ←  │ HTTP server     │
│ inventory│    │ pipeline   │    │ index    │    │ + CLI + library │
└──────────┘    └────────────┘    └──────────┘    └─────────────────┘
```

The build streams the inventory once, aggregates object count + bytes
+ per-storage-class stats at every prefix depth, and writes a columnar
index where one prefix occupies the same byte offset in every file.
Lookups are a single mmap read after an O(1) minimal-perfect-hash
probe. See [docs/overview.md](docs/overview.md) for the pipeline and
[docs/index-format.md](docs/index-format.md) for the on-disk layout.

## Quick start

```bash
git clone https://github.com/eunmann/s3-inv-db.git
cd s3-inv-db
make dev        # hot-reload server + MinIO at http://localhost:8080
```

`make dev` is the supported dev loop —
[`infra/docker-compose.yml`](infra/docker-compose.yml) brings up the
server under Air alongside a MinIO instance preloaded by the seeder.
Full details and the test workflow in [docs/dev.md](docs/dev.md).

## Binaries

```bash
make all     # bin/s3-inv-db (CLI) + bin/s3-inv-db-server
make seeder  # bin/s3-inv-db-seeder
```

| Binary | Purpose |
|---|---|
| `s3-inv-db` | CLI: `build`, `query`, `top`, `browse`, `compare`, `verify`, `stats`, `config-check` (every subcommand supports `--output text\|json`) |
| `s3-inv-db-server` | HTTP server: discovery, load/unload, auto-load, compare, JSON API, HTML UI, `/metrics` |
| `s3-inv-db-seeder` | Synthetic-data generator for local dev and integration tests |

## Configuration

Both `s3-inv-db` and `s3-inv-db-server` accept `--config <path>`
pointing at a JSON file shared between them. Precedence is **explicit
CLI flag → config file → default**. The seeder is flag-only.

```json
{
  "addr": ":8080",
  "s3_source": "s3://my-bucket/inventory-data/",
  "cache_dir": "/var/cache/s3inv",
  "auto_load": true,
  "max_index_disk": "200GB",
  "max_concurrent_jobs": 2,
  "auto_load_poll_interval": "15m",
  "auto_load_retention_default": 3,
  "discovery_refresh_interval": "1m",
  "query_batch_max": 1000,
  "build_event_log": "/var/log/s3inv-build.jsonl",
  "inventories": [
    {"source": "prod-bucket", "name": "daily-inventory", "auto_load": true, "retention_count": 5}
  ]
}
```

`inventories[]` declares per-configuration auto-load + retention;
entries are upserted into the state DB at startup. Per-run pin state
lives in the UI/API, not the file. The authoritative field list is
the `Config` struct in `internal/appconfig/appconfig.go`.

## Library usage

```go
import "github.com/eunmann/s3-inv-db/pkg/indexread"

idx, _ := indexread.Open("./my-index")
defer idx.Close()
stats, ok := idx.StatsForPrefix("data/2024/")
```

The HTTP server is not currently importable as a package. Full
reference: [docs/library-api.md](docs/library-api.md).

## Documentation

- [Overview](docs/overview.md) — system design, data flow, server behaviour
- [HTTP API](docs/http-api.md) — routes, JSON shapes, SSE
- [CLI](docs/cli.md) — every subcommand and its flags
- [Dev workflow](docs/dev.md) — Docker compose, Air, MinIO, tests
- [Seeding](docs/seeding.md) — synthetic-data generation
- [Index format](docs/index-format.md) — on-disk file layout
- [Library API](docs/library-api.md) — Go package reference
- [Performance](docs/performance.md) — memory, benchmarks, sizing

## Requirements

Go 1.26+, AWS credentials for build-time S3 access, Docker for the
dev/test stack.

## License

MIT
