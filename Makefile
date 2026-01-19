.PHONY: all build server seeder test test-race lint lint-fix clean clean-seed dev seed

GOLANGCI_LINT_VERSION := v2.1.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

AIR_VERSION := v1.61.7
AIR := go run github.com/air-verse/air@$(AIR_VERSION)

all: build server

build:
	go build -o bin/s3inv-index ./cmd/s3inv-index

server:
	go build -o bin/s3inv-server ./cmd/s3inv-server

seeder:
	go build -o bin/s3inv-seeder ./cmd/seeder

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

dev:
	$(AIR)

seed: seeder
	./bin/s3inv-seeder --out ./seed-data --count 3 --objects 10000 --verbose --pretty-logs
