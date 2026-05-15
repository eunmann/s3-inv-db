.PHONY: all build server seeder test test-race lint lint-check clean clean-seed seed \
        css dev docker-build docker-prod docker-seed docker-down \
        cover cover-html cover-summary tidy

GOLANGCI_LINT_VERSION := v2.9.0
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

COMPOSE := docker compose -f infra/docker-compose.yml

# Tailwind CLI: use the standalone binary release so we don't pull in
# Node.js for what's essentially a single tool. The binary is downloaded
# on first use and cached under bin/. Detect OS/arch automatically.
TAILWIND_VERSION := v3.4.17
UNAME_S := $(shell uname -s | tr '[:upper:]' '[:lower:]')
UNAME_M := $(shell uname -m)
TAILWIND_OS := $(if $(filter darwin,$(UNAME_S)),macos,linux)
TAILWIND_ARCH := $(if $(filter arm64 aarch64,$(UNAME_M)),arm64,x64)
TAILWIND_BIN := bin/tailwindcss-$(TAILWIND_VERSION)
TAILWIND_URL := https://github.com/tailwindlabs/tailwindcss/releases/download/$(TAILWIND_VERSION)/tailwindcss-$(TAILWIND_OS)-$(TAILWIND_ARCH)

CSS_INPUT  := internal/templates/styles/input.css
CSS_OUTPUT := internal/templates/styles/tailwind.css
CSS_CONFIG := tailwind.config.js
TEMPLATES  := $(shell find internal/templates/templates -name '*.html' 2>/dev/null)
HELPERS    := $(shell find internal/templates -maxdepth 1 -name '*.go' 2>/dev/null)

all: css build server

# Build Tailwind CSS from input.css + the project's templates. The
# generated tailwind.css is checked into the repo so production builds
# (e.g. infra/Dockerfile) don't need to fetch the CLI.
css: $(CSS_OUTPUT)

$(CSS_OUTPUT): $(CSS_INPUT) $(CSS_CONFIG) $(TEMPLATES) $(HELPERS) | $(TAILWIND_BIN)
	$(TAILWIND_BIN) -c $(CSS_CONFIG) -i $(CSS_INPUT) -o $(CSS_OUTPUT) --minify

$(TAILWIND_BIN):
	@mkdir -p bin
	curl -sSL -o $@.tmp "$(TAILWIND_URL)"
	chmod +x $@.tmp
	mv $@.tmp $@

build:
	go build -o bin/s3-inv-db ./cmd/s3-inv-db

server: css
	go build -o bin/s3-inv-db-server ./cmd/s3-inv-db-server

seeder:
	go build -o bin/s3-inv-db-seeder ./cmd/s3-inv-db-seeder

test:
	@$(COMPOSE) --profile test run --rm test-runner go test ./...; \
	rc=$$?; \
	$(COMPOSE) --profile test down -v >/dev/null 2>&1; \
	exit $$rc

test-race:
	@$(COMPOSE) --profile test run --rm test-runner go test -race ./...; \
	rc=$$?; \
	$(COMPOSE) --profile test down -v >/dev/null 2>&1; \
	exit $$rc

# Coverage profile. Writes one .out file the other coverage targets
# consume. Run `make cover-summary` for a one-line-per-package report
# or `make cover-html` to open the line-level browser view.
COVER_OUT := coverage.out

cover: $(COVER_OUT)

$(COVER_OUT):
	@$(COMPOSE) --profile test run --rm test-runner \
	    go test -covermode=atomic -coverpkg=./... -coverprofile=$(COVER_OUT) ./...; \
	rc=$$?; \
	$(COMPOSE) --profile test down -v >/dev/null 2>&1; \
	exit $$rc

cover-summary: $(COVER_OUT)
	@go tool cover -func=$(COVER_OUT) | tail -n 1
	@echo "--- 20 lowest-covered functions (gaps to consider): ---"
	@go tool cover -func=$(COVER_OUT) | awk '/^total:/ {next} {print}' | sort -k3 -n | head -20

cover-html: $(COVER_OUT)
	go tool cover -html=$(COVER_OUT) -o coverage.html
	@echo "open coverage.html"

# Keep go.mod tidy and verify no vendored drift snuck in.
tidy:
	go mod tidy
	go mod verify

# Default `lint` runs with --fix so iterative dev cleans up auto-fixable
# issues (gofumpt/gci/goimports/some style). CI should call `lint-check`
# instead so the build fails on anything --fix would have rewritten.
lint:
	$(GOLANGCI_LINT) run --fix ./...

lint-check:
	$(GOLANGCI_LINT) run ./...

clean:
	rm -rf bin/ tmp/ $(COVER_OUT) coverage.html

clean-seed:
	rm -rf seed-data/

seed: seeder
	./bin/s3-inv-db-seeder --out ./seed-data --count 3 --objects 10000 --verbose --pretty-logs

# ----- docker compose ------------------------------------------------------

# Build both dev and prod images.
docker-build:
	$(COMPOSE) --profile dev --profile prod build

# Hot-reload server (port 8080). The only supported dev workflow — no host
# binary path. Override port with S3INV_DEV_PORT=...
dev:
	$(COMPOSE) --profile dev up --build

# Slim production image (port 8081). Override with S3INV_PROD_PORT=...
docker-prod:
	$(COMPOSE) --profile prod up --build

# One-shot seeder: writes to ./seed-data on the host.
# Override count/objects/preset with S3INV_SEED_COUNT, S3INV_SEED_OBJECTS,
# S3INV_SEED_PRESET.
docker-seed:
	$(COMPOSE) --profile seed run --rm seeder

# Stop everything and remove volumes (caches).
docker-down:
	$(COMPOSE) --profile dev --profile prod --profile seed --profile test down -v
