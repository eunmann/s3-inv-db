# Dev workflow

The dev workflow runs in Docker — no host-binary path. The
[`infra/docker-compose.yml`](../infra/docker-compose.yml) file carries
four profiles: `dev`, `prod`, `seed`, and `test`.

## Boot the stack

```bash
make dev        # hot-reload server on :8080 + local S3 (versitygw) on :9000
```

[Air](https://github.com/air-verse/air) watches `*.go`, `*.html`,
and `*.tmpl` (see [`.air.toml`](../.air.toml)) and rebuilds the
binary on save. There is no in-process template-reload path — Air
covers it through the rebuild.

The local S3 server is [versitygw](https://github.com/versity/versitygw)
with its posix backend (dev credentials `devaccess` / `devsecret`);
`s3-setup` creates the `s3-inv` bucket with a SigV4-signed `curl`.
MinIO's images were withdrawn from Docker Hub, so the stack no longer
uses them.

```bash
make docker-prod  # slim production image on :8081
make docker-down  # tear everything down + remove volumes
```

## Hand-drive the API

After `make dev` boots:

```bash
# Generate synthetic inventories into the local S3 server
make docker-seed

# Browse discovered inventories
curl -s http://localhost:8080/api/discovered | jq

# Compare two loaded runs of the same configuration
curl -s 'http://localhost:8080/api/compare?from=src/inv/runA&to=src/inv/runB' | jq

# Trigger a load via the partial route (returns HTML for the swapped row)
curl -X POST \
  -H "Origin: http://localhost:8080" \
  http://localhost:8080/partials/discovered/synthetic-prod/inv-001/2026-05-13T03-02Z/load
```

Open `http://localhost:8080` for the HTMX UI.

## Tests

```bash
make test-unit    # host: go test ./... — no Docker, no network
make test         # full suite, including S3 integration tests, in Docker
make test-race    # same with -race
```

Tests that need S3 live in `*_s3_test.go` files behind the
`integration` build tag, so plain `go test ./...` on the host never
makes an AWS call. `make test` and `make test-race` run inside the
`test-runner` container with `-tags integration` against a dedicated
`s3-test` instance — no host ports, runs side-by-side with
`make dev`. The Makefile builds the test-runner image once (cached
after first run), captures the test exit code, and tears the profile
down on every exit.

`internal/s3test` refuses to run unless `AWS_ENDPOINT_URL_S3` is set
to a non-AWS host, so a misconfigured run fails instead of creating
buckets in a real account.

To iterate on one package, drive compose directly:

```bash
docker compose -f infra/docker-compose.yml --profile test run --rm test-runner \
  go test -tags integration -v ./internal/s3disco/...
```

## Local quality gates

`make help` lists every target. The gates PR Validation runs, each
callable locally:

| Target | Checks |
|---|---|
| `make lint-check` | golangci-lint (config in `.golangci.yml`, `integration` tag included); `make lint` applies auto-fixes |
| `make test-unit` / `make test` / `make test-race` | host unit tests / full suite in Docker / full suite with `-race` |
| `make cover-check` | host unit-test coverage ≥ `COVER_MIN` (a ratchet: raise it, never lower it) |
| `make deadcode` | no function unreachable from every binary and test |
| `make vuln` | govulncheck: no reachable known vulnerability |
| `make tidy-check` | `go.mod`/`go.sum` are tidy |
| `make css-check` | the checked-in `tailwind.css` matches the templates |
