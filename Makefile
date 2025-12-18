.PHONY: all build test test-race clean

all: build

build:
	go build -o bin/s3inv-index ./cmd/s3inv-index

test:
	go test ./...

test-race:
	go test -race ./...

clean:
	rm -rf bin/
