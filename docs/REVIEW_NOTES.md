# Code Review Notes

This document tracks the findings from a comprehensive code review and the refactoring work done.

## Completed Improvements

### Phase 1: Critical Bug Fixes ✅
- [x] Fix error handling in aggregator.Close() - now properly cleans up statements and documents intentional error ignoring
- [x] Document stmt.Close() errors in aggregator.go Commit/Rollback as intentionally ignored (best-effort cleanup)
- [x] Fix race condition in pipeline.go firstErr tracking - added atomic.Bool hasErr for race-free early termination
- [x] Use errors.Is() for SQL error comparisons instead of direct equality

### Phase 2: Reduce Duplication ✅
- [x] Extract shared CSV reader configuration to csvutil.go:newInventoryReader()
- [x] Extract common gzip decompression to csvutil.go:decompressReader()
- [x] Eliminated duplicate code in streamer.go, parallel.go, and pipeline.go

---

## Remaining Work (Lower Priority)

### Packages to Simplify
- [ ] **pkg/sqliteagg** - 3 overlapping streaming implementations (streamer.go, parallel.go, pipeline.go)
- [ ] **pkg/sqliteagg/indexbuild.go** - Could be separate from SQLite aggregation logic
- [ ] **pkg/logging** - LogEvent duplicates zerolog.Event functionality

### APIs to Tighten
- [ ] Standardize constructor names: Open() for file-backed, New() for in-memory
- [ ] Consider making PrefixRow, PrefixIterator internal
- [ ] Document when to use Pipeline vs ParallelStreamer vs single-threaded

### Tests to Add/Strengthen
- [ ] Concurrent access tests for Index (claims thread-safe)
- [ ] Benchmarks comparing the three streaming implementations
- [ ] Error scenario tests (S3 failures, corrupted CSV)
- [ ] Concurrent write serialization tests for Aggregator

### Phase 3: Package Organization
- [ ] Document when to use each streaming implementation
- [ ] Clean up unused/dead code paths

### Phase 4: Polish
- [ ] Reduce logging noise (rate limit progress logs)
- [ ] Standardize error context messages
- [ ] Add missing godoc documentation
