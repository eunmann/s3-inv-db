# Library API

`pkg/indexread` is the supported external entry point for reading a
built index. Other `pkg/*` packages (`format`, `extsort`, `s3fetch`,
…) are importable but their surfaces target the internal build
pipeline rather than third-party callers — expect churn.

The HTTP server lives in `internal/server` and is not currently
importable.

For the full signature surface use `go doc` or
[pkg.go.dev](https://pkg.go.dev/github.com/eunmann/s3-inv-db/pkg/indexread).
This page documents the things that aren't obvious from signatures
alone.

## Example

```go
import "github.com/eunmann/s3-inv-db/pkg/indexread"

idx, err := indexread.Open("./my-index")
if err != nil {
    log.Fatal(err)
}
defer idx.Close()

stats, ok := idx.StatsForPrefix("data/2024/")
if !ok {
    return // prefix not present in the index
}
fmt.Printf("%d objects, %d bytes\n", stats.ObjectCount, stats.TotalBytes)
```

## Method surface (groups)

- **Lookup**: `Lookup`, `StatsForPrefix`, `Stats`, `PrefixString`,
  `ChildSegment` (the last segment of the prefix at a position, without
  the trailing slash, for listing labels without building the full
  string).
- **Tree navigation**: `Depth`, `HasChildren`, `Count`,
  `MaxDepth`.
- **Descendants**: `DescendantsAtDepth`, `DescendantsAtDepthFiltered`
  (apply a `Filter{MinCount, MinBytes}` to skip cheap nodes before
  materialising positions), `DescendantsPage` (one `[offset,
  offset+limit)` window of the filtered matches plus their total,
  holding only the page in memory).
- **Top-N**: `TopByBytes`, `TopByCount`, `TopFiltered`, and
  `RankDescendants` (either direction: largest or smallest first). They
  read the depth-block bounds and then about k blocks of stats.
- **Per-tier**: `HasTierData`, `TierBreakdown`, `TierBreakdownMap`.

## Non-obvious invariants

- **Lifetime**: `Index` holds mmap regions; they stay valid until
  `Close`. Don't retain `[]byte` views obtained from internal helpers
  past that point.
- **Concurrency**: every method on `*Index` is safe for concurrent
  reads. Call `Close` exactly once, after all readers have stopped.
- **Misses are not errors**: `Lookup` and `StatsForPrefix` return
  `ok=false` for an absent prefix. `Stats` on an out-of-range position
  returns the zero `Stats{}`. `DescendantsAt*` and `Top*` return
  `(nil, nil)` and `DescendantsPage` an empty page for out-of-bounds
  inputs. `Open`, `Close` and `PrefixString` are the methods that
  surface errors in normal use; the `error` from `DescendantsAt*`,
  `DescendantsPage` and `Top*` is non-nil only when the depth index on
  disk is malformed.
- **Positions are in level order**: the root, then every depth-1
  prefix, then every depth-2 prefix, each depth in key order. A
  prefix's descendants at one depth are a contiguous range
  (`DescendantSpan`), in key order. See
  [index-format.md](index-format.md#level-order-positions).
- **Root and tier presence**: position 0 is the root prefix `""`.
  `HasTierData` is false only for an index with no prefixes; a build
  over an inventory without a storage-class column counts every object
  as `STANDARD`.
