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
curl -s 'http://localhost:8080/api/compare?from=src/inv/runA&to=src/inv/runB' | jq

# Trigger a load via the partial route (returns HTML for the swapped row)
curl -X POST \
  -H "Origin: http://localhost:8080" \
  http://localhost:8080/partials/discovered/synthetic-prod/inv-001/2026-05-13T03-02Z/load
```

Open `http://localhost:8080` for the HTMX UI.

## Tests

```bash
make test         # full suite in the docker test profile
make test-race    # same with -race
```

Both targets run inside the `test-runner` container with a dedicated
`minio-test` MinIO instance — no host ports, runs side-by-side with
`make dev`. The Makefile builds the test-runner image once (cached
after first run), captures the test exit code, and tears the profile
down on every exit.

To iterate on one package, drive compose directly:

```bash
docker compose -f infra/docker-compose.yml --profile test run --rm test-runner \
  go test -v ./internal/s3disco/...
```

Running `go test ./...` directly on the host bypasses MinIO and will
fail the s3disco integration tests by design — that's the workflow the
container exists to prevent.

## Local quality gates

```bash
make lint           # golangci-lint v2 (govet, staticcheck, errcheck, …)
make test           # full suite, dockerised
make test-race      # same with -race
make cover-summary  # total coverage + 20 lowest-covered functions
make tidy           # go mod tidy + go mod verify
```

The CI signal is `make lint && make test && make test-race`. See
`.golangci.yml` for the enabled-linter list.
