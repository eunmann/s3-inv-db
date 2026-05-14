# s3-inv-db

A high-performance indexer + HTTP server for S3 inventory reports. Builds a
compact, memory-mapped index that enables O(1) prefix lookups and fast
subtree aggregation queries, and exposes the queries through both a JSON
API and an HTMX-driven SSR web UI.

## Features

- **O(1) prefix lookups** using minimal perfect hashing (BBHash)
- **Memory-mapped queries** with sub-millisecond latency
- **Bounded memory builds** via external sort with configurable budget
- **Per-tier storage statistics** for all 12 S3 storage classes
- **Subtree aggregation** queries by depth with filtering
- **HTTP server with HTMX SSR UI** for browsing prefixes, loading
  inventories, viewing per-prefix cost estimates
- **S3 discovery** — server walks an `s3://bucket/inventory-prefix/`
  source and surfaces every inventory + its latest run
- **Asynchronous inventory loading** — Build & Load returns immediately
  with a queued job; pipeline progress and ETA stream to the row over
  Server-Sent Events. In-flight builds can be cancelled mid-pipeline.
- **Restart-safe** — inventory state and job history persist to SQLite
  (`$CACHE_DIR/state.db`); a fresh server process rehydrates the
  in-memory Manager and shows aborted jobs with a Retry button.
- **Pure Go** implementation with no CGO dependencies

## Binaries

The repo builds three binaries, all named for the project:

| Binary | Source | Purpose |
|---|---|---|
| `s3-inv-db` | `cmd/s3-inv-db` | CLI for `build` / `query` operations on a single index |
| `s3-inv-db-server` | `cmd/s3-inv-db-server` | HTTP server: discovery, load/unload, JSON API, HTML UI |
| `s3-inv-db-seeder` | `cmd/s3-inv-db-seeder` | Synthetic-data generator for local dev + integration tests |

## Installation

```bash
go install github.com/eunmann/s3-inv-db/cmd/s3-inv-db@latest
```

Or build all three from source:

```bash
git clone https://github.com/eunmann/s3-inv-db.git
cd s3-inv-db
make all        # builds bin/s3-inv-db + bin/s3-inv-db-server
make seeder     # builds bin/s3-inv-db-seeder
```

## CLI Quick Start

### Build an Index

```bash
s3-inv-db build \
  --s3-manifest s3://my-bucket/inventory/data/manifest.json \
  --out ./my-index
```

### Query the Index

```bash
# Basic prefix lookup
s3-inv-db query --index ./my-index --prefix "data/2024/"

# With tier breakdown and cost estimate
s3-inv-db query --index ./my-index --prefix "data/2024/" \
  --show-tiers --estimate-cost
```

## HTTP Server

`s3-inv-db-server` exposes the same query surface as the CLI plus an
SSR web UI driven by [HTMX](https://htmx.org/) — page actions
(load/unload/evict an inventory, drill into a prefix) submit via
`hx-post`/`hx-get` and the server returns HTML row/level partials that
swap in place. No JSON-and-reload anti-patterns.

### Run

```bash
# Standalone, listening on :8080, no discovery configured:
s3-inv-db-server

# With discovery — the server walks the s3:// source and shows every
# inventory + its load state in the UI.
s3-inv-db-server \
  --addr :8080 \
  --s3-source s3://my-bucket/inventory-data/ \
  --cache-dir /var/cache/s3inv
```

### Routes

| Route | Method | Returns | Notes |
|---|---|---|---|
| `/` | GET | HTML | Dashboard — counters + recent inventories |
| `/inventories` | GET | HTML | Discovery list (when `--s3-source` is set) |
| `/browse` | GET | HTML or row partial | Prefix explorer; content-negotiated on `HX-Request` |
| `/healthz` | GET | text/plain `ok` | Liveness probe (no auth, no S3) |
| `/static/tailwind.css` | GET | text/css | Embedded compiled stylesheet, ETag-revalidated |
| `/partials/inventory-row/{id}` | GET | HTML row | Single row of the (non-discovered) inventory list |
| `/partials/inventories/{id}/load` | POST | HTML row | Loads + returns updated row |
| `/partials/inventories/{id}/unload` | POST | HTML row | Unloads + returns updated row |
| `/partials/inventories/{id}` | DELETE | empty | Deletes (htmx outerHTML removes row) |
| `/partials/discovered/{src}/{id}` | GET | HTML row | Latest state of one discovered row (used by SSE refresh) |
| `/partials/discovered/{src}/{id}/load` | POST | HTML row, **202** | Submits an async build job; row swaps to queued state |
| `/partials/discovered/{src}/{id}/unload` | POST | HTML row | Unload |
| `/api/jobs/stream` | GET | `text/event-stream` | Server-Sent Events — one frame per job state change, with a `: ping` heartbeat |
| `/api/jobs/{id}/cancel` | POST | empty, **202** | Cancels an in-flight job |
| `/api/inventories` | GET, POST | JSON | List / register |
| `/api/inventories/{id}` | GET, DELETE | JSON | Get / delete |
| `/api/inventories/{id}/load`, `/unload` | POST | JSON | Load / unload |
| `/api/inventories/{id}/stats`, `/descendants` | GET | JSON | Per-prefix stats / depth-walk |
| `/api/discovered` | GET | JSON | List discovered inventories (when configured) |
| `/api/stats` | GET | JSON | Stats for `?inventory_id=&prefix=` |
| `/api/configurations` | GET | JSON | Inventory configurations grouped by `<src>/<inv>` with their runs |
| `/api/browse` | GET | JSON | Browse one prefix in one run (`?inventory_id=&prefix=&sort=&dir=&page=&page_size=`) |
| `/api/diff` | GET | JSON | Compare two runs at one prefix (`?from=&to=&prefix=&sort=&dir=&page=&page_size=&show_unchanged=`) |

#### Async load flow

A click on **Load** for a discovered inventory:

1. `POST /partials/discovered/{src}/{id}/load` → 202 + the row in `queued` state, no waiting for the build pipeline.
2. The row's `hx-trigger="sse:row-{src}/{id}"` listens to `/api/jobs/stream` and re-fetches itself via `GET /partials/discovered/{src}/{id}` on every state change.
3. A spinner + stage label ("Downloading & parsing") + progress bar + ETA render alongside the row's State chip while the job is live.
4. **Cancel** posts to `/api/jobs/{job-id}/cancel`, the build context is signalled, the goroutine winds down, the row swaps back to the prior state with a **Retry** button.

#### Persistence

Inventory state + job history live in `$CACHE_DIR/state.db` (SQLite, WAL mode, pure-Go driver). On boot the server rehydrates the in-memory Manager from this file and marks any job left running by the previous process as `aborted`. Inventories caught mid-load get flipped to `error` so the UI shows Retry instead of a forever spinner.

Cross-origin mutating requests (POST/PUT/PATCH/DELETE) are rejected by
a same-origin middleware; reads are public.

## Library Usage

```go
import "github.com/eunmann/s3-inv-db/pkg/indexread"

idx, err := indexread.Open("./my-index")
if err != nil {
    log.Fatal(err)
}
defer idx.Close()

// O(1) prefix lookup
pos, ok := idx.Lookup("data/2024/")
if !ok {
    log.Fatal("prefix not found")
}

// Get statistics
stats := idx.Stats(pos)
fmt.Printf("Objects: %d, Bytes: %d\n", stats.ObjectCount, stats.TotalBytes)

// Get tier breakdown
if idx.HasTierData() {
    for _, tb := range idx.TierBreakdown(pos) {
        fmt.Printf("%s: %d objects\n", tb.TierName, tb.ObjectCount)
    }
}
```

## Local Development

The dev workflow runs in Docker — no host-binary path. The
`infra/docker-compose.yml` file carries three profiles: `dev`, `prod`,
`seed`.

```bash
# Hot-reload server on :8080 with Air watching go + html files. Pulls in
# MinIO + minio-setup so discovery works against a local S3.
make dev

# One-shot synthetic-data seeder. Uploads inventories to MinIO under
# the dev stack, or writes them to ./seed-data when --target=local.
make docker-seed

# Production-style slim image on :8081.
make docker-prod

# Tear everything down.
make docker-down
```

Air watches `*.go`, `*.html`, and `*.tmpl` (see `.air.toml`), so editing
a template re-runs the binary and re-loads the embedded templates.
There is no in-process devMode template reload.

### Hand-driving the API against the dev stack

After `make dev` boots:

```bash
# Generate synthetic inventories into MinIO
make docker-seed

# Browse discovered inventories
curl -s http://localhost:8080/api/discovered | jq

# Trigger a load via the partial route (returns HTML for the swapped row)
curl -X POST \
  -H "Origin: http://localhost:8080" \
  http://localhost:8080/partials/discovered/synthetic-prod/inv-001/load
```

Open `http://localhost:8080` in a browser to use the HTMX UI instead.

## Tests

```bash
make test       # unit + integration tests
make test-race  # same with the race detector
make lint       # golangci-lint v2
```

### MinIO-backed tests

A handful of tests in `internal/s3disco` and `internal/seeder` exercise
the real S3 client against MinIO. They skip when `AWS_ENDPOINT_URL_S3`
is unset, so plain `go test ./...` runs cleanly without containers.
To exercise them, run inside the dev stack:

```bash
docker compose -f infra/docker-compose.yml --profile dev run --rm server-dev \
  go test ./internal/s3disco/... ./internal/seeder/...
```

## Architecture

The repo is organised in three layers:

- **HTTP layer** (`internal/server`, `internal/handlers`, `internal/templates`)
  parses requests, calls domain methods, renders JSON or HTML.
- **Domain layer** (`internal/inventory`, `internal/s3disco`, `internal/loader`)
  owns the use cases: inventory state machine, discovery, register+build+load
  orchestration. The HTTP layer never reaches past this boundary.
- **Storage / index layer** (`pkg/indexread`, `pkg/format`, `pkg/extsort`,
  `pkg/triebuild`) owns the mmap-backed on-disk format and the build
  pipeline.

Logging is **request-scoped via zerolog**: `hlog.NewHandler` attaches
the base logger to ctx and `zerolog.Ctx(r.Context())` retrieves it
inside handlers. No custom context-logger wrapper package — the
upstream zerolog/hlog integration is the source of truth.

### Index Format

```
my-index/
├── manifest.json         # File checksums and metadata
├── subtree_end.u64       # Preorder subtree ranges
├── depth.u32             # Prefix depths
├── object_count.u64      # Object counts per prefix
├── total_bytes.u64       # Byte totals per prefix
├── max_depth_in_subtree.u32
├── depth_offsets.u64     # Depth index for range queries
├── depth_positions.u64
├── mph.bin               # BBHash MPHF
├── mph_fp.u64            # Fingerprints for verification
├── mph_pos.u64           # Position mapping
├── prefix_blob.bin       # Concatenated prefix strings
├── prefix_offsets.u64    # Offsets into prefix blob
└── tier_stats/           # Per-tier statistics (optional)
    ├── tier_0_count.u64
    ├── tier_0_bytes.u64
    └── ...
```

## Documentation

- [Overview](docs/overview.md) - System design and data flow
- [Index Format](docs/index-format.md) - On-disk format specification
- [CLI Reference](docs/cli.md) - Command-line interface
- [Library API](docs/library-api.md) - Go package documentation
- [Performance](docs/performance.md) - Benchmarks and tuning

## Synthetic Seed Data

For local development and integration testing, generate synthetic
inventory indexes with `s3-inv-db-seeder`:

```bash
# Generate 3 inventories with 10K objects each, on disk
make seed

# Custom: 5 inventories, 50K objects, deeper trees
./bin/s3-inv-db-seeder --out ./seed-data --count 5 --objects 50000 --preset large

# Upload to MinIO (or any S3 endpoint via AWS_ENDPOINT_URL_S3)
./bin/s3-inv-db-seeder --target s3 --s3-bucket s3-inv --count 3
```

### Presets

| Preset | Fanout | Max Depth | Description |
|---|---|---|---|
| small | 5 | 3 | Compact test data |
| medium | 10 | 5 | Moderate complexity |
| large | 20 | 8 | Deep, wide trees |
| realistic | 15 | 7 | S3-like path patterns (default) |

## Requirements

- Go 1.25+
- AWS credentials configured for S3 access (build only)
- S3 inventory configured in CSV or Parquet format
- Docker + docker compose for the dev/test stack (optional, but
  recommended)

## License

MIT
