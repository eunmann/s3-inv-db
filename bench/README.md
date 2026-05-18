# Bench result conventions

All benchmark output artifacts live here. Convention:

```
bench/
  baselines/    # tagged baselines that survive across commits
  before/       # pre-change snapshots for the current item
  after/        # post-change snapshots for the current item
```

## Naming

Per item ID (Q5, I3, etc.):

```
bench/before/<id>-<bench>.txt
bench/after/<id>-<bench>.txt
```

Example: `bench/before/i3-pipeline_matrix.txt`,
`bench/after/i3-pipeline_matrix.txt`.

## How to capture

```sh
# Before change
go test -bench=<pattern> -benchtime=<n>x -count=<k> ./<pkg>/... \
  | tee bench/before/<id>-<short>.txt

# After change
go test -bench=<pattern> -benchtime=<n>x -count=<k> ./<pkg>/... \
  | tee bench/after/<id>-<short>.txt

# Diff
benchstat bench/before/<id>-<short>.txt bench/after/<id>-<short>.txt
```

## Reproducibility rules

- RNG seeded (use `benchutil` defaults — seed 42)
- Benches encode the shape × size × dict knob combination in the
  function name (e.g. `BenchmarkBuildHarness_DeepPyramid_1M_DictOn`);
  pick a comparison directly with `-bench=<pattern>`
- Use `-count` ≥ 3 for benchstat to compute variance
- `-benchtime` chosen so per-iter cost > 1 ms to swamp setup overhead
- Bench output uses Go's standard format (one row per bench/n combo)
- Pipeline integration benches under `internal/loader/` require
  `AWS_ENDPOINT_URL_S3` (the docker compose `test` profile)

## Convention for commits

Any commit claiming a perf change MUST cite both files in the
message. Example:

```
I3: pipe Merge → Build, eliminate disk round-trip

Pre/post:  bench/before/i3-pipeline_matrix.txt vs bench/after/...
benchstat: 1M-pipeline -18% wall, no peak heap or disk delta
```

## Cleanup

Files in `before/` and `after/` are scratch; the long-lived
artifacts are in `baselines/`. Stale before/after files can be
deleted as new items move through.
