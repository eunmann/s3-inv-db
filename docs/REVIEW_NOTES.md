# Code Review Notes

This document tracks the findings from a comprehensive code review and the refactoring work done.

## Packages to Simplify

- [ ] **pkg/sqliteagg** - 3 overlapping streaming implementations (streamer.go, parallel.go, pipeline.go)
- [ ] **pkg/sqliteagg/indexbuild.go** - Should be separate from SQLite aggregation logic
- [ ] **pkg/logging** - LogEvent duplicates zerolog.Event functionality

## Duplication to Remove

- [ ] CSV reader configuration repeated 3x (streamer, parallel, pipeline)
- [ ] S3 streaming + gzip decompression repeated 3x
- [ ] Tier mapping from S3 storage class repeated 3x
- [ ] Progress logging patterns repeated in multiple places

## APIs to Tighten

- [ ] Standardize constructor names: Open() for file-backed, New() for in-memory
- [ ] Consider making PrefixRow, PrefixIterator internal
- [ ] Document when to use Pipeline vs ParallelStreamer vs single-threaded

## Tests to Add/Strengthen

- [ ] Concurrent access tests for Index (claims thread-safe)
- [ ] Benchmarks comparing the three streaming implementations
- [ ] Error scenario tests (S3 failures, corrupted CSV)
- [ ] Concurrent write serialization tests for Aggregator

## Potential Risky Changes (Do Carefully)

- [ ] Race condition fix in pipeline.go error tracking
- [ ] Resource cleanup changes in aggregator.go (Close, Commit, Rollback)
- [ ] Consolidating streaming implementations (behavior must match)

---

## Refactoring Progress

### Phase 1: Critical Bug Fixes
- [ ] Fix error handling in aggregator.Close() - rollback error discarded
- [ ] Check stmt.Close() errors in aggregator.go Commit/Rollback
- [ ] Fix race condition in pipeline.go firstErr tracking
- [ ] Log parse errors instead of silently treating as 0

### Phase 2: Reduce Duplication
- [ ] Extract shared CSV reader configuration helper
- [ ] Extract common S3 streaming + decompression logic
- [ ] Consolidate tier mapping setup

### Phase 3: Package Organization
- [ ] Document when to use each streaming implementation
- [ ] Clean up unused/dead code paths

### Phase 4: Polish
- [ ] Reduce logging noise (rate limit progress logs)
- [ ] Standardize error context messages
- [ ] Add missing godoc documentation
