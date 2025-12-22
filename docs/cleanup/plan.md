# Cleanup Plan

This document outlines the cleanup plan for the s3-inv-db repository.

## Repository Map

### Top-Level Structure

```
s3-inv-db/
├── cmd/                    # Main CLI application
│   └── s3inv-index/        # Build and query command
├── internal/               # Internal packages (not exported)
│   ├── cli/                # CLI implementation
│   └── logctx/             # Context-based logging utilities
├── pkg/                    # Public library packages
│   ├── benchutil/          # Benchmark helpers (test-only)
│   ├── extsort/            # External sort pipeline
│   ├── format/             # Index format reader/writer
│   ├── humanfmt/           # Human-readable formatting
│   ├── indexread/          # Index reading/querying
│   ├── inventory/          # CSV/Parquet inventory parsing
│   ├── logging/            # Logging utilities
│   ├── membudget/          # Memory budget management
│   ├── memdiag/            # Memory diagnostics (pprof)
│   ├── pricing/            # S3 storage cost estimation
│   ├── s3fetch/            # S3 client and manifest handling
│   ├── sysmem/             # System memory detection
│   ├── tiers/              # Storage tier definitions
│   └── triebuild/          # Trie builder helpers
├── docs/                   # Documentation
│   ├── perf/               # Performance analysis docs
│   │   └── results/        # Benchmark results
│   └── cleanup/            # This cleanup documentation
├── Makefile                # Build automation
├── .golangci.yml           # Linter configuration
├── go.mod / go.sum         # Go module files
├── README.md               # Project readme
└── CLAUDE.md               # AI assistant guidelines
```

### Main Binaries/Commands

| Command | Path | Description |
|---------|------|-------------|
| s3inv-index | cmd/s3inv-index | Main CLI for building and querying indexes |

### Package Dependencies

```
cmd/s3inv-index
└── internal/cli
    ├── internal/logctx
    ├── pkg/extsort
    │   ├── pkg/format
    │   │   ├── pkg/logging
    │   │   ├── pkg/tiers
    │   │   └── pkg/triebuild
    │   ├── pkg/humanfmt
    │   ├── pkg/inventory
    │   │   └── pkg/tiers
    │   ├── pkg/membudget
    │   │   └── pkg/sysmem
    │   ├── pkg/memdiag
    │   ├── pkg/s3fetch
    │   └── pkg/tiers
    ├── pkg/indexread
    │   └── pkg/format
    ├── pkg/logging
    ├── pkg/membudget
    ├── pkg/pricing
    │   └── pkg/format
    ├── pkg/s3fetch
    └── pkg/sysmem
```

**Note:** `pkg/benchutil` is only used by test/benchmark files - this is intentional.

---

## Current State Assessment

### Linter Status

- **Command:** `make lint` (golangci-lint v2.1.2)
- **Result:** 0 issues
- **Linters enabled:** unused, unparam, ineffassign, errcheck, govet, staticcheck, and many more (see .golangci.yml)

### Test Status

- **Command:** `make test`
- **Result:** All tests pass
- **Coverage:** Packages without tests: cmd/s3inv-index, pkg/benchutil, pkg/memdiag

### go vet Status

- **Command:** `go vet ./...`
- **Result:** Clean (no issues)

---

## Cleanup Tasks

### Code Cleanup

- [x] Run golangci-lint - 0 issues found
- [x] Run go vet - clean
- [x] Verify all packages are reachable from main binary - confirmed
- [ ] Remove stray test binaries from root directory

#### Root Directory Cleanup

The following files appear to be stray test binaries that should be removed:
- `extsort.test` - compiled test binary
- `format.test` - compiled test binary

These are likely left over from running tests with `-c` flag.

#### Untracked Benchmark File

- `pkg/format/mphf_fingerprints_bench_test.go` - new benchmark file (untracked in git)

This file should be reviewed and either committed or removed.

### Documentation Cleanup

#### Core Documentation (docs/)

| File | Status | Notes |
|------|--------|-------|
| architecture.md | ACTIVE | Current, matches codebase |
| building.md | ACTIVE | Current, matches codebase |
| concurrency.md | ACTIVE | Current, matches codebase |
| context-and-signals.md | ACTIVE | Current, matches codebase |
| index-format.md | ACTIVE | Current, matches codebase |
| logging-guidelines.md | ACTIVE | Current, matches codebase |
| memory-detection.md | ACTIVE | Current, matches codebase |
| parquet-inventory.md | ACTIVE | Current, matches codebase |
| querying.md | ACTIVE | Current, matches codebase |

#### Performance Documentation (docs/perf/)

Many files in docs/perf/ are untracked in git (new analysis work):

| File | Status | Notes |
|------|--------|-------|
| e2e_benchmark_profile.md | UNTRACKED | Recent perf work |
| e2e_next_steps.md | UNTRACKED | Recent perf work |
| mphf_fingerprints_next_steps.md | UNTRACKED | Recent perf work |
| mphf_fingerprints_path.md | UNTRACKED | Recent perf work |
| mphf_fingerprints_profile.md | UNTRACKED | Recent perf work |
| mphf_fingerprints_summary.md | UNTRACKED | Recent perf work |
| mphf_next_steps.md | TRACKED | Committed |
| mphf_path.md | TRACKED | Committed |
| mphf_pgo.md | TRACKED | Committed |
| mphf_profile.md | TRACKED | Committed |
| pipeline_overview.md | UNTRACKED | Recent perf work |
| pipeline_summary.md | UNTRACKED | Recent perf work |
| stage_aggregate.md | UNTRACKED | Recent perf work |
| stage_build_index.md | UNTRACKED | Recent perf work |
| stage_download.md | UNTRACKED | Recent perf work |
| stage_manifest.md | UNTRACKED | Recent perf work |
| stage_merge.md | UNTRACKED | Recent perf work |
| stage_parse.md | UNTRACKED | Recent perf work |
| stage_query.md | UNTRACKED | Recent perf work |

**Recommendation:** Review untracked perf docs and either commit them or move to an archive.

#### Performance Results (docs/perf/results/)

| File | Status | Notes |
|------|--------|-------|
| s2-r1.md | TRACKED | Benchmark results |
| s3-r1.md | TRACKED | Benchmark results |
| s3-r3.md | TRACKED | Benchmark results |
| s5-r3.md | TRACKED | Benchmark results |

---

## Tooling Used

1. **golangci-lint v2.1.2** - Primary linting tool with extensive configuration
2. **go vet** - Additional static analysis
3. **go list** - Package dependency analysis
4. **go test** - Test execution verification
5. **grep/search** - Manual code reference validation

---

## Checklist

### Code Cleanup
- [x] Run golangci-lint - no issues
- [x] Run go vet - clean
- [x] Verify package import graph - all packages used
- [x] Look for unused functions/types - none found (linter clean)
- [ ] Remove stray test binaries (extsort.test, format.test)
- [ ] Review untracked benchmark file

### Documentation Cleanup
- [x] Inventory all documentation files
- [x] Classify docs as ACTIVE/STALE/OBSOLETE
- [ ] Review untracked perf docs
- [ ] Decide on untracked docs: commit or archive

### Final Steps
- [ ] Run make lint after changes
- [ ] Run make test after changes
- [ ] Create summary document
