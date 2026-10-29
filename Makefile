.PHONY: help all build server seeder test test-unit test-race lint lint-check clean clean-seed seed \
        css css-check dev dev-reset docker-build docker-prod docker-seed docker-down \
        cover cover-html cover-summary cover-check tidy tidy-check deadcode vuln actionlint bench-ab bench-ingest bench-query bench-scale pgo compose-preflight

.DEFAULT_GOAL := help

GOLANGCI_LINT_VERSION := v2.15.0
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
DEADCODE := go run golang.org/x/tools/cmd/deadcode@v0.52.0
GOVULNCHECK := go run golang.org/x/vuln/cmd/govulncheck@v1.9.0
ACTIONLINT := go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.7

# Unit-test coverage floor (host suite, no S3 server). Ratchet it up as
# coverage grows; never lower it to make a change pass.
COVER_MIN := 67.0

COMPOSE := docker compose -f infra/docker-compose.yml
# The test stack gets a project name per checkout, so `make test` in two
# worktrees at once doesn't tear down the other's containers.
TEST_PROJECT ?= s3inv-test-$(shell printf %s "$(CURDIR)" | cksum | cut -d' ' -f1)
# Test containers run on the CPUs the caller may use (its taskset
# affinity), so a pinned caller keeps them off pinned benchmark cores.
TEST_CPUSET ?= $(shell taskset -cp $$$$ 2>/dev/null | sed -n 's/.*list: //p')
TEST_COMPOSE := S3INV_TEST_CPUSET=$(TEST_CPUSET) docker compose -p $(TEST_PROJECT) -f infra/docker-compose.yml

# Version metadata stamped into every binary (see internal/buildinfo).
# The release workflow overrides all three. DATE is the commit time, not
# the build time, so rebuilding a commit gives the same binary. The go
# command's own VCS stamp is not enough: it is absent in git worktrees.
VERSION ?= $(shell git describe --tags --match 'v[0-9]*' --always --dirty 2>/dev/null)
COMMIT  ?= $(shell git describe --always --dirty --abbrev=40 --exclude='*' 2>/dev/null)
DATE    ?= $(shell TZ=UTC git log -1 --date=format-local:%Y-%m-%dT%H:%M:%SZ --format=%cd 2>/dev/null)
BUILDINFO := github.com/eunmann/s3-inv-db/internal/buildinfo
LDFLAGS := -X $(BUILDINFO).version=$(VERSION) -X $(BUILDINFO).commit=$(COMMIT) -X $(BUILDINFO).date=$(DATE)
GO_BUILD := go build -trimpath -ldflags "$(LDFLAGS)"

# Tailwind CLI: use the standalone binary release so we don't pull in
# Node.js for what's essentially a single tool. The binary is downloaded
# on first use, checked against the pinned sha256 (the release's
# sha256sums.txt) and cached under bin/. Detect OS/arch automatically.
TAILWIND_VERSION := v3.4.17
UNAME_S := $(shell uname -s | tr '[:upper:]' '[:lower:]')
UNAME_M := $(shell uname -m)
TAILWIND_OS := $(if $(filter darwin,$(UNAME_S)),macos,linux)
TAILWIND_ARCH := $(if $(filter arm64 aarch64,$(UNAME_M)),arm64,x64)
TAILWIND_BIN := bin/tailwindcss-$(TAILWIND_VERSION)
TAILWIND_URL := https://github.com/tailwindlabs/tailwindcss/releases/download/$(TAILWIND_VERSION)/tailwindcss-$(TAILWIND_OS)-$(TAILWIND_ARCH)
TAILWIND_SHA256_linux-x64   := 7d24f7fa191d2193b78cd5f5a42a6093e14409521908529f42d80b11fde1f1d4
TAILWIND_SHA256_linux-arm64 := 69b1378b8133192d7d2feb12a116fa12d035594f58db3eff215879e4ad8cf39b
TAILWIND_SHA256_macos-x64   := 6cbdad74be776c087ffa5e9a057512c54898f9fe8828d3362212dfe32fc933a3
TAILWIND_SHA256_macos-arm64 := a1d0c7985759accca0bf12e51ac1dcbf0f6cf2fffb62e6e0f62d091c477a10a3
TAILWIND_SHA256 := $(TAILWIND_SHA256_$(TAILWIND_OS)-$(TAILWIND_ARCH))
SHA256SUM := $(if $(filter darwin,$(UNAME_S)),shasum -a 256,sha256sum)

CSS_INPUT  := internal/templates/styles/input.css
CSS_OUTPUT := internal/templates/styles/tailwind.css
CSS_CONFIG := tailwind.config.js
TEMPLATES  := $(shell find internal/templates/templates -name '*.html' 2>/dev/null)
HELPERS    := $(shell find internal/templates -maxdepth 1 -name '*.go' 2>/dev/null)

help: ## List targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

all: css build server ## Build the CLI and server (regenerates CSS)

# Build Tailwind CSS from input.css + the project's templates. The
# generated tailwind.css is checked into the repo (PR Validation runs
# css-check) so production builds (infra/Dockerfile, the release
# workflow's `make -o css`) never fetch or run the CLI.
css: $(CSS_OUTPUT) ## Regenerate tailwind.css from the templates

css-check: ## Fail if the checked-in tailwind.css is stale
	@$(MAKE) --no-print-directory -B css >/dev/null 2>&1
	@git diff --exit-code --stat -- $(CSS_OUTPUT) || { echo "tailwind.css is stale: run make css and commit it"; exit 1; }

$(CSS_OUTPUT): $(CSS_INPUT) $(CSS_CONFIG) $(TEMPLATES) $(HELPERS) | $(TAILWIND_BIN)
	$(TAILWIND_BIN) -c $(CSS_CONFIG) -i $(CSS_INPUT) -o $(CSS_OUTPUT) --minify

$(TAILWIND_BIN):
	@mkdir -p bin
	curl -fsSL -o $@.tmp "$(TAILWIND_URL)"
	echo "$(TAILWIND_SHA256)  $@.tmp" | $(SHA256SUM) -c - || { rm -f $@.tmp; exit 1; }
	chmod +x $@.tmp
	mv $@.tmp $@

build: ## Build bin/s3-inv-db
	$(GO_BUILD) -o bin/s3-inv-db ./cmd/s3-inv-db

server: css ## Build bin/s3-inv-db-server
	$(GO_BUILD) -o bin/s3-inv-db-server ./cmd/s3-inv-db-server

seeder: ## Build bin/s3-inv-db-seeder
	$(GO_BUILD) -o bin/s3-inv-db-seeder ./cmd/s3-inv-db-seeder

test-unit: ## Host unit tests (no Docker, no network)
	go test ./...

test: compose-preflight ## Full suite incl. S3 integration tests, in Docker
	@$(TEST_COMPOSE) --profile test run --rm test-runner go test -tags integration ./...; \
	rc=$$?; \
	$(TEST_COMPOSE) --profile test down -v >/dev/null 2>&1; \
	exit $$rc

test-race: compose-preflight ## Full suite with -race, in Docker
	@$(TEST_COMPOSE) --profile test run --rm test-runner go test -tags integration -race ./...; \
	rc=$$?; \
	$(TEST_COMPOSE) --profile test down -v >/dev/null 2>&1; \
	exit $$rc

# Coverage profile. Writes one .out file the other coverage targets
# consume. Run `make cover-summary` for a one-line-per-package report
# or `make cover-html` to open the line-level browser view.
COVER_OUT := coverage.out

cover: $(COVER_OUT) ## Full-suite coverage profile (Docker)

$(COVER_OUT): compose-preflight
	@$(TEST_COMPOSE) --profile test run --rm test-runner \
	    go test -tags integration -covermode=atomic -coverpkg=./... -coverprofile=$(COVER_OUT) ./...; \
	rc=$$?; \
	$(TEST_COMPOSE) --profile test down -v >/dev/null 2>&1; \
	exit $$rc

cover-summary: $(COVER_OUT) ## Coverage total + 20 least-covered functions
	@go tool cover -func=$(COVER_OUT) | tail -n 1
	@echo "--- 20 lowest-covered functions (gaps to consider): ---"
	@go tool cover -func=$(COVER_OUT) | awk '/^total:/ {next} {print}' | sort -k3 -n | head -20

cover-check: ## Fail if host unit-test coverage is below COVER_MIN
	@mkdir -p tmp
	@go test -count=1 -covermode=atomic -coverpkg=./... -coverprofile=tmp/cover-unit.out ./... >/dev/null
	@go tool cover -func=tmp/cover-unit.out | awk -v min="$(COVER_MIN)" \
		'END { gsub(/%/,"",$$3); if ($$3+0 < min+0) { printf "FAIL: coverage %s%% < COVER_MIN %s%%\n", $$3, min; exit 1 } \
		printf "OK: coverage %s%% >= %s%%\n", $$3, min }'

cover-html: $(COVER_OUT) ## Line-level coverage HTML
	go tool cover -html=$(COVER_OUT) -o coverage.html
	@echo "open coverage.html"

# Keep go.mod tidy and verify no vendored drift snuck in.
tidy: ## go mod tidy + verify
	go mod tidy
	go mod verify

tidy-check: ## Fail if go.mod/go.sum are not tidy
	go mod tidy
	git diff --exit-code -- go.mod go.sum

deadcode: ## Fail on functions unreachable from every binary and test
	@out="$$($(DEADCODE) -test -tags integration ./...)"; \
	if [ -n "$$out" ]; then echo "$$out"; echo "deadcode: unreachable functions found"; exit 1; fi

vuln: ## govulncheck (reachable vulnerabilities only)
	$(GOVULNCHECK) ./...

actionlint: ## Lint GitHub Actions workflows
	$(ACTIONLINT)

# A/B benchmark gate. Run only for ingestion or data-structure/query
# changes, and only that area's suite — see bench/README.md. Not in CI.
BASE ?= main
ROUNDS ?= 8
bench-ingest: ## A/B the ingestion suite (BASE vs working tree)
	scripts/bench-ab.sh -n $(ROUNDS) -s bench/suite/ingest.txt $(BASE)

bench-query: ## A/B the data-structure/query suite (BASE vs working tree)
	scripts/bench-ab.sh -n $(ROUNDS) -s bench/suite/query.txt $(BASE)

SCALE_ROUNDS ?= 4
bench-scale: ## A/B the scale suite (10M objects; long): make bench-scale BASE=<ref>
	S3INV_LONG_BENCH=1 scripts/bench-ab.sh -n $(SCALE_ROUNDS) -s bench/suite/scale.txt $(BASE)

pgo: ## Regenerate the PGO profiles (cmd/*/default.pgo) from the query benchmarks
	scripts/pgo-profile.sh

bench-ab: ## A/B a custom suite: make bench-ab SUITE=<file> BASE=<ref>
	@test -n "$(SUITE)" || { echo "SUITE is required (or use bench-ingest / bench-query)"; exit 2; }
	scripts/bench-ab.sh -n $(ROUNDS) -s $(SUITE) $(BASE)

# Default `lint` runs with --fix so iterative dev cleans up auto-fixable
# issues (gofumpt/gci/goimports/some style). CI should call `lint-check`
# instead so the build fails on anything --fix would have rewritten.
lint: ## golangci-lint with --fix
	$(GOLANGCI_LINT) run --fix ./...

lint-check: ## golangci-lint without fixes (CI)
	$(GOLANGCI_LINT) run ./...

clean: ## Remove build outputs
	rm -rf bin/ tmp/ $(COVER_OUT) coverage.html

clean-seed:
	rm -rf seed-data/

seed: seeder ## Generate local synthetic inventories
	./bin/s3-inv-db-seeder --out ./seed-data --count 3 --objects 10000 --verbose --pretty-logs

# ----- docker compose ------------------------------------------------------

# Detect compose containers whose attached network no longer exists, and tear
# them down so the next `up` can recreate them. This is the standard failure
# mode after `docker network prune`, a Docker daemon restart, or a host
# reboot: containers survive with a dangling network reference, and the next
# `compose up` fails with "network <id> not found". Named volumes are
# preserved (no `-v`), so caches and local S3 data are kept.
compose-preflight:
	@stale=""; \
	for c in $$($(COMPOSE) ps -aq 2>/dev/null); do \
	    for net in $$(docker inspect -f '{{range $$k,$$v := .NetworkSettings.Networks}}{{$$k}} {{end}}' $$c 2>/dev/null); do \
	        if ! docker network inspect "$$net" >/dev/null 2>&1; then \
	            stale=1; break 2; \
	        fi; \
	    done; \
	done; \
	if [ -n "$$stale" ]; then \
	    echo "compose-preflight: stale network reference detected — clearing containers (volumes preserved)"; \
	    $(COMPOSE) --profile dev --profile prod --profile seed --profile test down --remove-orphans >/dev/null 2>&1 || true; \
	fi

# Build both dev and prod images.
docker-build: css ## Build dev + prod images
	$(COMPOSE) --profile dev --profile prod build

# Hot-reload server (port 8080). The only supported dev workflow — no host
# binary path. Override port with S3INV_DEV_PORT=..., the index disk
# budget (default 8GB) with S3INV_DEV_MAX_INDEX_DISK=...
dev: compose-preflight ## Hot-reload server + local S3 (the dev loop)
	$(COMPOSE) --profile dev up --build

# Wipe the dev server's persistent state (state.db + downloaded indexes)
# without wiping the local S3 bucket. Use when the schema gets wedged, the
# auto-loader is stuck in a backoff loop, or you just want a clean slate.
# Re-running `make dev` after this re-applies every migration from scratch
# and rebuilds caches on demand.
dev-reset: ## Wipe the dev server's state volume
	$(COMPOSE) --profile dev stop server-dev
	$(COMPOSE) --profile dev rm -f server-dev
	docker volume rm -f infra_server-cache
	@echo "dev state cleared — run 'make dev' to start fresh"

# Slim production image (port 8081). Override with S3INV_PROD_PORT=...
docker-prod: css compose-preflight ## Prod image on :8081
	$(COMPOSE) --profile prod up --build

# One-shot seeder: writes to ./seed-data on the host.
# Override count/objects/preset with S3INV_SEED_COUNT, S3INV_SEED_OBJECTS,
# S3INV_SEED_PRESET.
docker-seed: compose-preflight ## One-shot seeder into ./seed-data
	$(COMPOSE) --profile seed run --rm seeder

# Stop everything and remove volumes (caches).
docker-down: ## Stop all profiles and remove volumes
	$(COMPOSE) --profile dev --profile prod --profile seed --profile test down -v
