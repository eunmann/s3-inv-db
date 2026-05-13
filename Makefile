.PHONY: all build server seeder test test-race lint lint-fix clean clean-seed seed \
        dev docker-build docker-prod docker-seed docker-down

GOLANGCI_LINT_VERSION := v2.1.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

COMPOSE := docker compose -f infra/docker-compose.yml

all: build server

build:
	go build -o bin/s3-inv-db ./cmd/s3-inv-db

server:
	go build -o bin/s3-inv-db-server ./cmd/s3-inv-db-server

seeder:
	go build -o bin/s3-inv-db-seeder ./cmd/s3-inv-db-seeder

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	$(GOLANGCI_LINT) run ./...

lint-fix:
	$(GOLANGCI_LINT) run --fix ./...

clean:
	rm -rf bin/ tmp/

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
	$(COMPOSE) --profile dev --profile prod --profile seed down -v
