# Documentation Inventory

This document catalogs all documentation in the s3-inv-db repository.

## Classification Key

| Status | Meaning |
|--------|---------|
| **ACTIVE** | Current, accurate, useful |
| **STALE_NEEDS_UPDATE** | Still relevant but partially outdated |
| **OBSOLETE** | No longer relevant, should be removed/archived |
| **UNTRACKED** | Not yet committed to git |

---

## Root Documentation

| File | Status | Purpose | Notes |
|------|--------|---------|-------|
| README.md | ACTIVE | Project introduction | Current, includes quick start and docs links |
| CLAUDE.md | ACTIVE | AI assistant workflow guidelines | Current, defines contribution workflow |

---

## Core Documentation (docs/)

### User-Facing Documentation

| File | Status | Purpose | Notes |
|------|--------|---------|-------|
| architecture.md | ACTIVE | System design overview | Accurate diagrams and component descriptions |
| building.md | ACTIVE | Build pipeline usage | Accurate CLI examples and programmatic usage |
| index-format.md | ACTIVE | On-disk format specification | Matches current implementation |
| querying.md | ACTIVE | Query API documentation | Complete with examples and performance notes |

### Developer Documentation

| File | Status | Purpose | Notes |
|------|--------|---------|-------|
| concurrency.md | ACTIVE | Concurrency architecture | Accurate worker pool description |
| context-and-signals.md | ACTIVE | Context/signal handling | Current cancellation patterns |
| logging-guidelines.md | ACTIVE | Logging conventions | Matches zerolog usage in codebase |
| memory-detection.md | ACTIVE | Memory budget system | Accurate pkg/membudget documentation |
| parquet-inventory.md | ACTIVE | Parquet format support | Current feature documentation |

---

## Performance Documentation (docs/perf/)

### Committed Files

| File | Status | Purpose | Notes |
|------|--------|---------|-------|
| mphf_next_steps.md | ACTIVE | MPHF optimization roadmap | Historical context, still relevant |
| mphf_path.md | ACTIVE | MPHF optimization path | Historical context for decisions |
| mphf_pgo.md | ACTIVE | PGO optimization notes | Profile-guided optimization analysis |
| mphf_profile.md | ACTIVE | MPHF profiling analysis | Baseline performance data |

### Untracked Files (Recent Work)

These files appear to be from recent performance analysis work and are not yet committed:

| File | Status | Purpose | Recommendation |
|------|--------|---------|----------------|
| e2e_benchmark_profile.md | UNTRACKED | End-to-end profiling | Review and commit |
| e2e_next_steps.md | UNTRACKED | E2E optimization plan | Review and commit |
| mphf_fingerprints_next_steps.md | UNTRACKED | Fingerprint optimization plan | Review and commit |
| mphf_fingerprints_path.md | UNTRACKED | Fingerprint optimization path | Review and commit |
| mphf_fingerprints_profile.md | UNTRACKED | Fingerprint profiling | Review and commit |
| mphf_fingerprints_summary.md | UNTRACKED | Fingerprint optimization summary | Review and commit |
| pipeline_overview.md | UNTRACKED | Pipeline performance overview | Review and commit |
| pipeline_summary.md | UNTRACKED | Pipeline optimization summary | Review and commit |
| stage_aggregate.md | UNTRACKED | Aggregation stage analysis | Review and commit |
| stage_build_index.md | UNTRACKED | Index build stage analysis | Review and commit |
| stage_download.md | UNTRACKED | Download stage analysis | Review and commit |
| stage_manifest.md | UNTRACKED | Manifest stage analysis | Review and commit |
| stage_merge.md | UNTRACKED | Merge stage analysis | Review and commit |
| stage_parse.md | UNTRACKED | Parse stage analysis | Review and commit |
| stage_query.md | UNTRACKED | Query stage analysis | Review and commit |

### Benchmark Results (docs/perf/results/)

| File | Status | Purpose | Notes |
|------|--------|---------|-------|
| s2-r1.md | ACTIVE | Sprint 2 Round 1 results | Historical benchmark data |
| s3-r1.md | ACTIVE | Sprint 3 Round 1 results | Historical benchmark data |
| s3-r3.md | ACTIVE | Sprint 3 Round 3 results | Historical benchmark data |
| s5-r3.md | ACTIVE | Sprint 5 Round 3 results | Historical benchmark data |

---

## Cleanup Documentation (docs/cleanup/)

| File | Status | Purpose |
|------|--------|---------|
| plan.md | ACTIVE | This cleanup plan |
| code_unused.md | ACTIVE | Unused code analysis results |
| docs_inventory.md | ACTIVE | This document |
| docs_changes.md | ACTIVE | Documentation change log |
| summary.md | ACTIVE | Final cleanup summary |

---

## Documentation Quality Assessment

### Strengths

1. **Comprehensive coverage** - All major features are documented
2. **Consistent structure** - Similar format across docs
3. **Code examples** - Good use of runnable examples
4. **Up-to-date** - Core docs match current implementation

### Areas for Improvement

1. **Untracked perf docs** - Many recent performance documents not committed
2. **No versioning** - Docs don't indicate which version they apply to

---

## Recommendations

### Immediate Actions

1. **Review untracked perf docs** - Decide whether to commit or archive
2. **Add .gitignore rule** - Prevent test binaries from appearing again

### Future Improvements

1. **Add version tags** - Indicate applicable version in docs
2. **Add last-updated dates** - Help readers assess freshness
3. **Link related docs** - Cross-reference between related topics
