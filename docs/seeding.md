# Synthetic seed data

`s3-inv-db-seeder` generates synthetic inventories so the rest of the
stack has data to work against — useful for local development,
integration tests, and benchmarks.

## Generate locally

```bash
# Defaults: 3 inventories, 10K objects each, on disk under ./seed-data
make seed

# Custom run
./bin/s3-inv-db-seeder \
  --out ./seed-data \
  --count 5 \
  --objects 50000 \
  --preset large
```

Each invocation writes one directory per inventory and one
manifest.json that mirrors the S3 inventory layout.

## Upload to MinIO

```bash
make docker-seed                       # via the dev stack
# or against any S3 endpoint:
./bin/s3-inv-db-seeder \
  --target s3 \
  --s3-bucket s3-inv \
  --count 3
```

## Multiple runs per configuration

The `--runs-per-inventory` flag generates several runs per inventory
configuration with staggered timestamps so the Inventories page
shows the grouping + run history out of the box. The `--run-step`
flag controls the gap between runs (default `24h`).

```bash
./bin/s3-inv-db-seeder \
  --target s3 \
  --runs-per-inventory 4 \
  --run-step 12h
```

`make docker-seed` defaults to `--runs-per-inventory=4` so the dev
stack has grouping content from the first boot.

## Presets

| Preset | Fanout | Max Depth | Description |
|---|---|---|---|
| `small` | 5 | 3 | Compact test data |
| `medium` | 10 | 5 | Moderate complexity |
| `large` | 20 | 8 | Deep, wide trees |
| `realistic` | 15 | 7 | S3-like path patterns (default) |
| `deep_pyramid` | 256 (leaf) | 10 | Narrow-top / wide-bottom shape with long descriptive segments — the regime where the prefix dictionary pays off |

## Mixed-size mixes

`--objects-per-config` overrides `--objects` per inventory configuration.
The list cycles modulo `--count`, so a single seeder invocation can
populate the UI with a mix of fast- and slow-loading inventories:

```bash
./bin/s3-inv-db-seeder \
  --target s3 \
  --count 6 \
  --objects-per-config 5000,500000,5000000
# inv-001/004 → 5K objects
# inv-002/005 → 500K objects
# inv-003/006 → 5M objects
```

## Deterministic output

The seeder is deterministic given the same `--seed`, `--count`, and
`--objects`. The dev stack uses a fixed seed by default so test
expectations don't drift between runs. Override with `--seed <n>`
for fresh data.
