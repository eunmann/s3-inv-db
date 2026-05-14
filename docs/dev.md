# Dev workflow

The dev workflow runs in Docker — no host-binary path. The
[`infra/docker-compose.yml`](../infra/docker-compose.yml) file carries
three profiles: `dev`, `prod`, `seed`.

## Boot the stack

```bash
make dev        # hot-reload server on :8080 + MinIO + minio-setup
```

[Air](https://github.com/air-verse/air) watches `*.go`, `*.html`,
and `*.tmpl` (see [`.air.toml`](../.air.toml)) and rebuilds the
binary on save. There is no in-process template-reload path — Air
covers it through the rebuild.

```bash
make docker-prod  # slim production image on :8081
make docker-down  # tear everything down + remove volumes
```

## Hand-drive the API

After `make dev` boots:

```bash
# Generate synthetic inventories into MinIO
make docker-seed

# Browse discovered inventories
curl -s http://localhost:8080/api/discovered | jq

# Compare two loaded runs of the same configuration
curl -s 'http://localhost:8080/api/diff?from=src/inv/runA&to=src/inv/runB' | jq

# Trigger a load via the partial route (returns HTML for the swapped row)
curl -X POST \
  -H "Origin: http://localhost:8080" \
  http://localhost:8080/partials/discovered/synthetic-prod/inv-001/2026-05-13T03-02Z/load
```

Open `http://localhost:8080` for the HTMX UI.

## MinIO-backed tests

A handful of tests under `internal/s3disco`, `internal/seeder`, and
`pkg/s3fetch` exercise the real S3 client against MinIO. They `t.Skip`
when `AWS_ENDPOINT_URL_S3` is unset, so plain `go test ./...` runs
cleanly without containers.

To exercise them inside the dev stack:

```bash
docker compose -f infra/docker-compose.yml --profile dev run --rm server-dev \
  go test ./internal/s3disco/... ./internal/seeder/... ./pkg/s3fetch/...
```

## Local quality gates

```bash
make lint           # golangci-lint v2 (govet, staticcheck, errcheck, …)
make test           # unit + integration (S3 paths skipped without env)
make test-race      # same with -race
make cover-summary  # total coverage + 20 lowest-covered functions
make tidy           # go mod tidy + go mod verify
```

The CI signal is `make lint && make test && make test-race`. See
`.golangci.yml` for the enabled-linter list.
