# CLI Reference

## Commands

```
s3inv-index <command> [options]

Commands:
  build    Build an index from S3 inventory
  query    Query an existing index
```

## build

Build an index from an S3 inventory manifest.

```bash
s3inv-index build [options]
```

### Required Flags

| Flag | Description |
|------|-------------|
| `--out` | Output directory for index files |
| `--s3-manifest` | S3 URI to inventory manifest.json |

### Optional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | CPU count | Concurrent S3 download/parse workers |
| `--max-depth` | 0 (unlimited) | Maximum prefix depth to track |
| `--mem-budget` | 50% of RAM | Memory budget (e.g., `4GiB`, `8GB`) |
| `--verbose` | false | Enable debug logging |
| `--pretty-logs` | false | Human-friendly console output |

### Examples

Basic build:
```bash
s3inv-index build \
  --s3-manifest s3://my-bucket/inventory/data/manifest.json \
  --out ./my-index
```

With memory constraint:
```bash
s3inv-index build \
  --s3-manifest s3://my-bucket/inventory/data/manifest.json \
  --out ./my-index \
  --mem-budget 4GiB
```

Limit prefix depth:
```bash
s3inv-index build \
  --s3-manifest s3://my-bucket/inventory/data/manifest.json \
  --out ./my-index \
  --max-depth 5
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `S3INV_MEM_BUDGET` | Memory budget (overridden by `--mem-budget`) |

Priority: `--mem-budget` > `S3INV_MEM_BUDGET` > auto-detect (50% of RAM)

### AWS Credentials

The build command requires AWS credentials with S3 read access. Credentials are loaded from the standard AWS SDK chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credentials file (`~/.aws/credentials`)
3. IAM role (EC2, ECS, Lambda)

Required S3 permissions:
- `s3:GetObject` on inventory files
- `s3:ListBucket` on inventory bucket (for manifest discovery)

## query

Query an existing index for prefix statistics.

```bash
s3inv-index query [options]
```

### Required Flags

| Flag | Description |
|------|-------------|
| `--index` | Index directory to query |
| `--prefix` | Prefix to look up |

### Optional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--show-tiers` | false | Show per-tier storage breakdown |
| `--estimate-cost` | false | Estimate monthly storage cost |
| `--price-table` | US East 1 | Path to custom price table JSON |
| `--verbose` | false | Enable debug logging |
| `--pretty-logs` | false | Human-friendly console output |

### Examples

Basic query:
```bash
s3inv-index query --index ./my-index --prefix "data/2024/"
```

Output:
```
Prefix: data/2024/
Objects: 1523456
Bytes: 847293847561
```

With tier breakdown:
```bash
s3inv-index query --index ./my-index --prefix "data/2024/" --show-tiers
```

Output:
```
Prefix: data/2024/
Objects: 1523456
Bytes: 847293847561

Tier breakdown:
  STANDARD: 1000000 objects, 500000000000 bytes
  GLACIER: 523456 objects, 347293847561 bytes
```

With cost estimate:
```bash
s3inv-index query --index ./my-index --prefix "data/2024/" \
  --show-tiers --estimate-cost
```

Output:
```
Prefix: data/2024/
Objects: 1523456
Bytes: 847293847561

Tier breakdown:
  STANDARD: 1000000 objects, 500000000000 bytes
  GLACIER: 523456 objects, 347293847561 bytes

Estimated monthly cost:
  Total: $12.34/month
  STANDARD: $11.50/month
  GLACIER: $0.84/month
```

### Custom Price Table

Create a JSON file with per-tier prices in microdollars per byte per month:

```json
{
  "STANDARD": 23000,
  "STANDARD_IA": 12500,
  "GLACIER": 4000,
  "DEEP_ARCHIVE": 1000
}
```

Then specify with `--price-table`:
```bash
s3inv-index query --index ./my-index --prefix "data/" \
  --estimate-cost --price-table ./prices.json
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (invalid arguments, S3 failure, etc.) |

## Signals

The build command handles `SIGINT` and `SIGTERM` gracefully, cleaning up temporary files before exit.
