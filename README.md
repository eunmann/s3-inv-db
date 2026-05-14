# s3-inv-db

High-performance indexer + HTTP server for [S3 Inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory.html)
reports. Builds a compact, memory-mapped index that answers O(1)
prefix lookups and fast subtree aggregations, and exposes it through
a JSON API and an HTMX SSR web UI.

```
┌──────────┐    ┌────────────┐    ┌──────────┐    ┌─────────────────┐
│ S3       │ →  │ build      │ →  │ mmap     │ ←  │ HTTP server     │
│ inventory│    │ pipeline   │    │ index    │    │ + CLI + library │
└──────────┘    └────────────┘    └──────────┘    └─────────────────┘
```

## Quick start

```bash
git clone https://github.com/eunmann/s3-inv-db.git
cd s3-inv-db
make dev        # hot-reload server + MinIO at http://localhost:8080
```

That's the only supported dev workflow — `make dev` boots the
[docker-compose](infra/docker-compose.yml) stack with Air watching
`*.go` and `*.html` so edits rebuild in seconds. See
[docs/dev.md](docs/dev.md) for the full local workflow,
[docs/seeding.md](docs/seeding.md) for synthetic data, and
[docs/http-api.md](docs/http-api.md) for the HTTP surface (routes,
JSON shapes, SSE).

## Build standalone binaries

```bash
make all     # bin/s3-inv-db (CLI) + bin/s3-inv-db-server
make seeder  # bin/s3-inv-db-seeder
```

| Binary | Purpose |
|---|---|
| `s3-inv-db` | CLI for `build` / `query` on a single index |
| `s3-inv-db-server` | HTTP server: discovery, load/unload, JSON API, HTML UI |
| `s3-inv-db-seeder` | Synthetic-data generator (local dev + integration tests) |

## Library usage

Read a built index directly:

```go
import "github.com/eunmann/s3-inv-db/pkg/indexread"

idx, err := indexread.Open("./my-index")
// ...
pos, ok := idx.Lookup("data/2024/")
stats := idx.Stats(pos)  // ObjectCount, TotalBytes
```

Or embed the full HTTP server in your own binary:

```go
import "github.com/eunmann/s3-inv-db/pkg/server"

err := server.BootstrapAndRun(ctx, server.RuntimeOptions{
    Addr:     ":8080",
    S3Source: "s3://my-bucket/inventory-data/",
    CacheDir: "/var/cache/s3inv",
    Logger:   logger,
})
```

The server exposes its chi router via `srv.Router()` so it can be mounted
behind your own middleware. Full API in [docs/library-api.md](docs/library-api.md).

## Testing & quality

```bash
make lint           # golangci-lint v2 — also runs govet, staticcheck, errcheck
make test           # full suite in docker (MinIO-backed integration tests included)
make test-race      # same with -race
make cover          # write coverage.out
make cover-summary  # total % + 20 lowest-covered functions
make cover-html     # open coverage.html in a browser
```

`make test` boots a dedicated `minio-test` container (no host ports,
runs side-by-side with `make dev` without colliding) and exercises the
S3 integration paths against it. There is no host-only path — running
`go test ./...` directly skips the dockerised setup and the integration
tests fail by design.

## Architecture

Three layers, dependency direction enforced top-down:

```
HTTP        internal/{server,handlers,templates}
              │
Domain      internal/{inventory,s3disco,loader,jobs}
              │
Storage     pkg/{indexread,format,extsort,triebuild,s3fetch}
```

- The HTTP layer never reaches past the domain boundary.
- The Inventory entity + typed `inventory.ID` live in `internal/inventory`
  — s3disco imports inventory, not the other way around.
- Logging is request-scoped via `zerolog` + `rs/zerolog/hlog`. No
  custom context-logger wrapper.

More in [docs/overview.md](docs/overview.md).

## Documentation

- [Overview](docs/overview.md) — system design, data flow, package layout
- [HTTP API](docs/http-api.md) — routes, JSON shapes, SSE, persistence
- [Dev workflow](docs/dev.md) — docker compose, Air, MinIO
- [Seeding](docs/seeding.md) — synthetic-data generation
- [CLI](docs/cli.md) — `s3-inv-db build` / `query`
- [Library API](docs/library-api.md) — Go package reference
- [Index format](docs/index-format.md) — on-disk layout
- [Performance](docs/performance.md) — benchmarks and tuning

## Requirements

Go 1.25+, AWS credentials for build-time S3 access, Docker for the
dev/test stack.

## License

MIT
