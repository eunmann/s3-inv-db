# Getting started

Install the binaries, build an index from one inventory run, query it,
then serve every run.

## Install

Each [release](https://github.com/eunmann/s3-inv-db/releases) ships,
for Linux and macOS on amd64 and arm64:

- `s3-inv-db`: the CLI ([reference](cli.md));
- `s3-inv-db-server`: the web UI and JSON API ([operations](operations.md));
- `s3-inv-db-seeder`: synthetic inventories for development
  ([seeding](seeding.md)).

```bash
VERSION=v0.1.0 OS=linux ARCH=amd64   # OS: linux|darwin, ARCH: amd64|arm64
ARCHIVE="s3-inv-db_${VERSION}_${OS}_${ARCH}.tar.gz"
BASE="https://github.com/eunmann/s3-inv-db/releases/download/$VERSION"
curl -fL -O "$BASE/$ARCHIVE" -O "$BASE/SHA256SUMS"
grep " $ARCHIVE\$" SHA256SUMS | sha256sum --check   # macOS: shasum -a 256 --check
tar -xzf "$ARCHIVE"
```

`gh attestation verify "$ARCHIVE" --repo eunmann/s3-inv-db` checks the
archive's signed build provenance. The macOS binaries are not
notarized; one downloaded with a browser needs
`xattr -d com.apple.quarantine s3-inv-db*` first.

The server also ships as a container image. It runs as a non-root user
on `:8080` (health check: `/healthz`) and keeps indexes and its state
database in `/var/cache/s3inv`, so mount a volume there. `latest` is
the newest non-pre-release.

```bash
docker run -d -p 8080:8080 -v s3-inv-db-cache:/var/cache/s3inv \
  -e AWS_REGION=us-east-1 -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
  ghcr.io/eunmann/s3-inv-db-server:v0.1.0 --s3-source s3://my-inventory-bucket/inventory/
```

From source (Go 1.27+): `go install github.com/eunmann/s3-inv-db/cmd/s3-inv-db@latest`
(and `.../cmd/s3-inv-db-server@latest`), or `make all seeder` in a
clone.

## Build and query an index

AWS writes each
[inventory](https://docs.aws.amazon.com/AmazonS3/latest/userguide/configure-inventory.html)
run to
`s3://<destination-bucket>/<destination-prefix>/<source-bucket>/<inventory-id>/<YYYY-MM-DDTHH-MMZ>/manifest.json`.
The binaries use the standard AWS credential chain and need
`s3:ListBucket` on the destination bucket and `s3:GetObject` on the
inventory files. Set `AWS_REGION` to that bucket's region.

```bash
export AWS_REGION=us-east-1
aws s3 ls s3://my-inventory-bucket/inventory/my-data-bucket/daily/   # pick a run
./s3-inv-db build \
  --s3-manifest s3://my-inventory-bucket/inventory/my-data-bucket/daily/2026-10-24T01-00Z/manifest.json \
  --out ./my-index
./s3-inv-db query --index ./my-index --prefix "logs/" --show-tiers --estimate-cost
./s3-inv-db top --index ./my-index --depth 1 --limit 10 --by bytes
```

## Serve every run

```json
{
  "s3_source": "s3://my-inventory-bucket/inventory/",
  "cache_dir": "./s3inv-cache",
  "auto_load": true,
  "max_index_disk": "50GB",
  "inventories": [{"source": "my-data-bucket", "name": "daily", "auto_load": true}]
}
```

```bash
./s3-inv-db config-check --config config.json
./s3-inv-db-server --config config.json   # http://localhost:8080
```

The server lists every run under `s3_source`. With `auto_load`, it
keeps the newest runs (two by default) of `my-data-bucket`/`daily`
built within the 50 GB budget; any other run loads from the
Inventories page. Next: [configuration](configuration.md),
[operations](operations.md).
