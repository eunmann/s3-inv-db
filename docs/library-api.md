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

- **Lookup**: `Lookup`, `StatsForPrefix`, `Stats`, `PrefixString`.
- **Tree navigation**: `Depth`, `SubtreeEnd`, `MaxDepthInSubtree`,
  `Count`, `MaxDepth`.
- **Descendants**: `DescendantsAtDepth`, `DescendantsAtDepthFiltered`
  (apply a `Filter{MinCount, MinBytes}` to skip cheap nodes before
  materialising positions).
- **Top-N**: `TopByBytes`, `TopByCount`, `TopFiltered`.
- **Per-tier**: `HasTierData`, `TierBreakdown`, `TierBreakdownMap`.

## Non-obvious invariants

- **Lifetime**: `Index` holds mmap regions; they stay valid until
  `Close`. Don't retain `[]byte` views obtained from internal helpers
  past that point.
- **Concurrency**: every method on `*Index` is safe for concurrent
  reads. Call `Close` exactly once, after all readers have stopped.
- **Misses are not errors**: `Lookup` and `StatsForPrefix` return
  `ok=false` for an absent prefix. `Stats` on an out-of-range position
  returns the zero `Stats{}`. `DescendantsAt*` returns `(nil, nil)`
  for out-of-bounds inputs. Only `PrefixString` and `Open` surface
  errors.
- **Subtree end is inclusive**: `SubtreeEnd(pos)` is the last
  descendant's position (the closed interval `[pos, SubtreeEnd(pos)]`),
  not a half-open upper bound. See
  [index-format.md](index-format.md#preorder-positions).
- **Tier presence is per-index**: `HasTierData` is false when no
  storage-class column was written for this index (very small or
  legacy builds).
