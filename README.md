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
index where one prefix occupies the same row in every per-prefix file.
Lookups are a single mmap read after an O(1) minimal-perfect-hash
probe. See [docs/overview.md](docs/overview.md) for the pipeline and
[docs/index-format.md](docs/index-format.md) for the on-disk layout.

## Install

Releases are listed on the
[releases page](https://github.com/eunmann/s3-inv-db/releases). Each
one ships the three binaries (below) for Linux and macOS on amd64 and
arm64, a `SHA256SUMS` file, and a multi-arch server image.

### Release archive

```bash
VERSION=v0.1.0 OS=linux ARCH=amd64   # OS: linux|darwin, ARCH: amd64|arm64
ARCHIVE="s3-inv-db_${VERSION}_${OS}_${ARCH}.tar.gz"
BASE="https://github.com/eunmann/s3-inv-db/releases/download/$VERSION"
curl -fL -O "$BASE/$ARCHIVE" -O "$BASE/SHA256SUMS"
grep " $ARCHIVE\$" SHA256SUMS | sha256sum --check   # macOS: shasum -a 256 --check
tar -xzf "$ARCHIVE"   # s3-inv-db, s3-inv-db-server, s3-inv-db-seeder, README.md
./s3-inv-db --version
```

Every archive also has signed build provenance; with the GitHub CLI,
`gh attestation verify "$ARCHIVE" --repo eunmann/s3-inv-db` checks that
it was built by this repository's release workflow. The macOS binaries
are not notarized: one downloaded with a browser needs
`xattr -d com.apple.quarantine s3-inv-db*` before it runs.

### Container image

```bash
docker run -d --name s3-inv-db -p 8080:8080 \
  -v s3-inv-db-cache:/var/cache/s3inv \
  -e AWS_REGION=us-east-1 -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
  ghcr.io/eunmann/s3-inv-db-server:v0.1.0 \
  --s3-source s3://my-inventory-bucket/inventory/
```

The image runs `s3-inv-db-server` as a non-root user, listening on
`:8080` (its health check probes `/healthz` there). Built indexes and
the state database live in `--cache-dir`, `/var/cache/s3inv` by
default, so mount a volume there to keep them across restarts. `latest`
tracks the newest release that is not a pre-release.

### From source

```bash
go install github.com/eunmann/s3-inv-db/cmd/s3-inv-db@latest
go install github.com/eunmann/s3-inv-db/cmd/s3-inv-db-server@latest
# or, from a clone, into bin/:
make all seeder
```

## Quick start with a real S3 inventory

AWS writes each
[inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/configure-inventory.html)
run to
`s3://<destination-bucket>/<destination-prefix>/<source-bucket>/<inventory-id>/<YYYY-MM-DDTHH-MMZ>/manifest.json`.
Both binaries use the standard AWS credential chain (environment,
shared credentials file, IAM role) and need `s3:ListBucket` on the
destination bucket and `s3:GetObject` on the inventory files. Set
`AWS_REGION` to the destination bucket's region.

Build an index from one run and query it with the CLI:

```bash
export AWS_REGION=us-east-1
aws s3 ls s3://my-inventory-bucket/inventory/my-data-bucket/daily/   # pick a run
./s3-inv-db build \
  --s3-manifest s3://my-inventory-bucket/inventory/my-data-bucket/daily/2026-10-24T01-00Z/manifest.json \
  --out ./my-index
./s3-inv-db query --index ./my-index --prefix "logs/" --show-tiers --estimate-cost
./s3-inv-db top --index ./my-index --depth 1 --limit 10 --by bytes
```

Or run the server over every inventory under the destination prefix,
with a minimal config file:

```json
{
  "s3_source": "s3://my-inventory-bucket/inventory/",
  "cache_dir": "./s3inv-cache",
  "auto_load": true,
  "max_index_disk": "50GB",
  "inventories": [
    {"source": "my-data-bucket", "name": "daily", "auto_load": true}
  ]
}
```

```bash
./s3-inv-db config-check --config config.json
./s3-inv-db-server --config config.json   # http://localhost:8080
```

The server discovers every run under `s3_source`. With `auto_load`, it
keeps the newest runs (two by default) of each inventory marked
`auto_load` (here `my-data-bucket`/`daily`) built, within the
`max_index_disk` budget; any other run loads from the Inventories page. Every key and flag is in
[Configuration](#configuration) and [docs/cli.md](docs/cli.md).

## Quick start

```bash
git clone https://github.com/eunmann/s3-inv-db.git
cd s3-inv-db
make dev        # hot-reload server + local S3 at http://localhost:8080
```

`make dev` is the supported dev loop —
[`infra/docker-compose.yml`](infra/docker-compose.yml) brings up the
server under Air alongside a local S3 server (versitygw).
Full details and the test workflow in [docs/dev.md](docs/dev.md).

## Binaries

```bash
make all     # bin/s3-inv-db (CLI) + bin/s3-inv-db-server
make seeder  # bin/s3-inv-db-seeder
```

| Binary | Purpose |
|---|---|
| `s3-inv-db` | CLI: `build`, `query`, `top`, `browse`, `compare`, `verify`, `stats`, `config-check` (every subcommand but `build` supports `--output text\|json`) |
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
  "trusted_proxies": ["10.0.0.0/8"],
  "build_event_log": "/var/log/s3inv-build.jsonl",
  "inventories": [
    {"source": "prod-bucket", "name": "daily-inventory", "auto_load": true, "retention_count": 5}
  ]
}
```

`inventories[]` declares per-configuration auto-load + retention;
entries are upserted into the state DB at startup.
`trusted_proxies` (flag `--trusted-proxies`) lists the reverse proxies
whose `X-Forwarded-For` is believed; by default none is, and the
client IP is the TCP peer. Per-run pin state
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
- [Dev workflow](docs/dev.md) — Docker compose, Air, local S3, tests
- [Seeding](docs/seeding.md) — synthetic-data generation
- [Index format](docs/index-format.md) — on-disk file layout
- [Library API](docs/library-api.md) — Go package reference
- [Performance](docs/performance.md) — memory, benchmarks, sizing
- [Releasing](docs/releasing.md) — dispatching a release or dry run

## Requirements

Go 1.27+, AWS credentials for build-time S3 access, Docker for the
dev/test stack.

## License

MIT, see [LICENSE](LICENSE).
