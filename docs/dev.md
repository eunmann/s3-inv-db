# Dev workflow

The dev workflow runs in Docker — no host-binary path. The
[`infra/docker-compose.yml`](../infra/docker-compose.yml) file carries
four profiles: `dev`, `prod`, `seed`, and `test`.

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

A handful of tests under `internal/s3disco` and `pkg/s3fetch` exercise
the real S3 client against MinIO. They `t.Skip` when
`AWS_ENDPOINT_URL_S3` is unset, so plain `go test ./...` runs cleanly
on the host but leaves those paths at 0% coverage.

The `test` profile fixes that: it boots a dedicated `minio-test` (no
host ports — runs side-by-side with `make dev` without colliding) and
runs `go test ./...` against it inside a container.

```bash
make docker-test   # boots minio-test, runs go test ./..., tears down
```

The Makefile target builds the test-runner image once (cached after
first run), captures the test exit code, and always tears the profile
down so containers don't linger.

To run a targeted subset (e.g. iterate on one package), drive compose
directly — `run --rm` replaces the default command:

```bash
docker compose -f infra/docker-compose.yml --profile test run --rm test-runner \
  go test -v ./internal/s3disco/...
```

## Local quality gates

```bash
make lint           # golangci-lint v2 (govet, staticcheck, errcheck, …)
make test           # unit + integration (S3 paths skipped without env)
make test-race      # same with -race
make docker-test    # full suite incl. MinIO-gated paths
make cover-summary  # total coverage + 20 lowest-covered functions
make tidy           # go mod tidy + go mod verify
```

The CI signal is `make lint && make test && make test-race`; add
`make docker-test` when MinIO-path coverage matters. See
`.golangci.yml` for the enabled-linter list.
