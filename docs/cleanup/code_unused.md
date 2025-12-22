# Unused Code Analysis

This document reports the results of the unused code analysis for s3-inv-db.

## Tooling Used

### golangci-lint (v2.1.2)

The following linters were enabled and checked for unused code:

| Linter | Purpose | Result |
|--------|---------|--------|
| `unused` | Detects unused code | Clean |
| `unparam` | Detects unused function parameters | Clean |
| `ineffassign` | Detects ineffective assignments | Clean |
| `deadcode` | Detects dead code (deprecated, replaced by unused) | N/A |
| `varcheck` | Detects unused variables (deprecated, replaced by unused) | N/A |
| `structcheck` | Detects unused struct fields (deprecated, replaced by unused) | N/A |

**Command:** `make lint`
**Result:** 0 issues

### go vet

**Command:** `go vet ./...`
**Result:** No issues detected

## Package Analysis

### All Packages

```
github.com/eunmann/s3-inv-db/cmd/s3inv-index      - Main binary entry point
github.com/eunmann/s3-inv-db/internal/cli         - CLI implementation
github.com/eunmann/s3-inv-db/internal/logctx      - Context-based logging
github.com/eunmann/s3-inv-db/pkg/benchutil        - Benchmark utilities (test-only)
github.com/eunmann/s3-inv-db/pkg/extsort          - External sort pipeline
github.com/eunmann/s3-inv-db/pkg/format           - Index format handling
github.com/eunmann/s3-inv-db/pkg/humanfmt         - Human-readable formatting
github.com/eunmann/s3-inv-db/pkg/indexread        - Index querying
github.com/eunmann/s3-inv-db/pkg/inventory        - Inventory file parsing
github.com/eunmann/s3-inv-db/pkg/logging          - Logging utilities
github.com/eunmann/s3-inv-db/pkg/membudget        - Memory budget management
github.com/eunmann/s3-inv-db/pkg/memdiag          - Memory diagnostics
github.com/eunmann/s3-inv-db/pkg/pricing          - S3 storage pricing
github.com/eunmann/s3-inv-db/pkg/s3fetch          - S3 manifest/file fetching
github.com/eunmann/s3-inv-db/pkg/sysmem           - System memory detection
github.com/eunmann/s3-inv-db/pkg/tiers            - Storage tier definitions
github.com/eunmann/s3-inv-db/pkg/triebuild        - Trie builder helpers
```

### Package Reachability

All packages are reachable from the main binary either directly or transitively:

- **Direct from cmd/s3inv-index:** internal/cli, pkg/logging
- **Via internal/cli:** internal/logctx, pkg/extsort, pkg/indexread, pkg/logging, pkg/membudget, pkg/pricing, pkg/s3fetch, pkg/sysmem
- **Transitive:** pkg/format, pkg/humanfmt, pkg/inventory, pkg/memdiag, pkg/tiers, pkg/triebuild

### Test-Only Package

**pkg/benchutil** is only imported by test files:
- `pkg/triebuild/bench_test.go`
- `pkg/indexread/bench_test.go`
- `pkg/indexread/bench_helpers_test.go`
- `pkg/extsort/bench_test.go`

This is intentional - the package provides benchmark utilities and is correctly used only in benchmarks.

## Unused Code Findings

### Functions/Types/Variables

**None found.** The golangci-lint `unused` linter reports 0 issues.

### Dead Code Paths

**None found.** Static analysis shows no unreachable code.

### Unused Parameters

**None found.** The `unparam` linter reports 0 issues.

## Files Removed

### Stray Test Binaries

The following compiled test binaries were found in the root directory and removed:

| File | Size | Removed |
|------|------|---------|
| `extsort.test` | 11.9 MB | Yes |
| `format.test` | 5.5 MB | Yes |

These were leftover from running `go test -c` commands.

## Conclusion

The codebase is clean with no unused code detected by automated tools. All packages serve a purpose and are reachable from the main binary. The only cleanup performed was removing stray test binary files from the root directory.
