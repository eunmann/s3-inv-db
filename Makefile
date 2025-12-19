.PHONY: all build test test-race lint lint-fix clean

GOLANGCI_LINT_VERSION := v2.1.2
GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

all: build

build:
	go build -o bin/s3inv-index ./cmd/s3inv-index

test:
	go test ./...

test-race:
	go test -race ./...

lint:
	$(GOLANGCI_LINT) run ./...

lint-fix:
	$(GOLANGCI_LINT) run --fix ./...

clean:
	rm -rf bin/
