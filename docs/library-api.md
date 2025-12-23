# Library API

The `indexread` package provides read-only access to S3 inventory indexes.

```go
import "github.com/eunmann/s3-inv-db/pkg/indexread"
```

## Opening an Index

```go
idx, err := indexread.Open("/path/to/index")
if err != nil {
    log.Fatal(err)
}
defer idx.Close()
```

The `Index` type is safe for concurrent read access from multiple goroutines.

## Index Type

### Metadata

```go
// Count returns the total number of prefixes in the index.
func (idx *Index) Count() uint64

// MaxDepth returns the maximum prefix depth in the index.
func (idx *Index) MaxDepth() uint32
```

### Prefix Lookup

```go
// Lookup returns the position for a prefix, or ok=false if not found.
// O(1) via MPHF.
func (idx *Index) Lookup(prefix string) (pos uint64, ok bool)
```

Example:
```go
pos, ok := idx.Lookup("data/2024/")
if !ok {
    fmt.Println("prefix not found")
    return
}
```

### Statistics

```go
// Stats returns object count and total bytes for the node at pos.
func (idx *Index) Stats(pos uint64) Stats

// StatsForPrefix combines Lookup and Stats.
func (idx *Index) StatsForPrefix(prefix string) (Stats, bool)
```

The `Stats` struct:
```go
type Stats struct {
    ObjectCount uint64
    TotalBytes  uint64
}
```

Example:
```go
stats, ok := idx.StatsForPrefix("data/2024/")
if ok {
    fmt.Printf("Objects: %d, Bytes: %d\n", stats.ObjectCount, stats.TotalBytes)
}
```

### Tree Navigation

```go
// Depth returns the depth of the node at pos.
func (idx *Index) Depth(pos uint64) uint32

// SubtreeEnd returns the exclusive end position of the subtree at pos.
func (idx *Index) SubtreeEnd(pos uint64) uint64

// MaxDepthInSubtree returns the maximum depth in the subtree at pos.
func (idx *Index) MaxDepthInSubtree(pos uint64) uint32

// PrefixString returns the prefix string for the node at pos.
func (idx *Index) PrefixString(pos uint64) (string, error)
```

### Descendant Queries

```go
// DescendantsAtDepth returns positions of descendants at exactly relDepth
// levels below the prefix at prefixPos. Returns them in alphabetical order.
func (idx *Index) DescendantsAtDepth(prefixPos uint64, relDepth int) ([]uint64, error)

// DescendantsUpToDepth returns positions grouped by depth, up to maxRelDepth
// levels below prefixPos.
func (idx *Index) DescendantsUpToDepth(prefixPos uint64, maxRelDepth int) ([][]uint64, error)

// DescendantsAtDepthFiltered returns filtered descendants.
func (idx *Index) DescendantsAtDepthFiltered(prefixPos uint64, relDepth int, filter Filter) ([]uint64, error)
```

The `Filter` struct:
```go
type Filter struct {
    MinCount uint64  // Minimum object count
    MinBytes uint64  // Minimum byte count
}
```

Example: Find immediate children with >1000 objects:
```go
pos, _ := idx.Lookup("data/")
children, err := idx.DescendantsAtDepthFiltered(pos, 1, indexread.Filter{
    MinCount: 1000,
})
if err != nil {
    log.Fatal(err)
}
for _, childPos := range children {
    prefix, _ := idx.PrefixString(childPos)
    stats := idx.Stats(childPos)
    fmt.Printf("%s: %d objects\n", prefix, stats.ObjectCount)
}
```

### Descendant Iterator

For large result sets, use an iterator to avoid allocating the full position slice:

```go
// NewDescendantIterator returns an iterator over descendants at relDepth.
func (idx *Index) NewDescendantIterator(prefixPos uint64, relDepth int) (Iterator, error)
```

The `Iterator` interface:
```go
type Iterator interface {
    Next() bool      // Advance to next position
    Pos() uint64     // Current position (valid after Next returns true)
    Depth() uint32   // Depth of current position
}
```

Example:
```go
pos, _ := idx.Lookup("data/")
it, err := idx.NewDescendantIterator(pos, 1)
if err != nil {
    log.Fatal(err)
}
for it.Next() {
    prefix, _ := idx.PrefixString(it.Pos())
    fmt.Println(prefix)
}
```

### Tier Statistics

```go
// HasTierData returns whether the index has per-tier statistics.
func (idx *Index) HasTierData() bool

// TierBreakdown returns per-tier stats for non-zero tiers at pos.
func (idx *Index) TierBreakdown(pos uint64) []TierBreakdown

// TierBreakdownAll returns stats for all tiers (including zeros).
func (idx *Index) TierBreakdownAll(pos uint64) []TierBreakdown

// TierBreakdownForPrefix combines Lookup and TierBreakdown.
func (idx *Index) TierBreakdownForPrefix(prefix string) []TierBreakdown

// TierBreakdownMap returns breakdown as a map keyed by tier name.
func (idx *Index) TierBreakdownMap(pos uint64) map[string]TierBreakdown
```

The `TierBreakdown` struct:
```go
type TierBreakdown struct {
    TierName    string
    ObjectCount uint64
    Bytes       uint64
}
```

Example:
```go
if idx.HasTierData() {
    for _, tb := range idx.TierBreakdownForPrefix("data/2024/") {
        fmt.Printf("%s: %d objects, %d bytes\n",
            tb.TierName, tb.ObjectCount, tb.Bytes)
    }
}
```

## Thread Safety

The `Index` type is safe for concurrent reads. All query methods can be called simultaneously from multiple goroutines. `Close()` should only be called once, after all reads have completed.

## Error Handling

- `Open` returns an error if any index file is missing or corrupted
- `Lookup` returns `ok=false` for missing prefixes (not an error)
- `Stats` returns zero values for out-of-bounds positions
- Descendant methods return `(nil, nil)` for invalid inputs (out of bounds, negative depth)
- `PrefixString` returns an error for invalid positions

## Complete Example

```go
package main

import (
    "fmt"
    "log"

    "github.com/eunmann/s3-inv-db/pkg/indexread"
)

func main() {
    idx, err := indexread.Open("./my-index")
    if err != nil {
        log.Fatal(err)
    }
    defer idx.Close()

    fmt.Printf("Index contains %d prefixes (max depth: %d)\n",
        idx.Count(), idx.MaxDepth())

    // Look up a prefix
    prefix := "data/2024/"
    pos, ok := idx.Lookup(prefix)
    if !ok {
        log.Fatalf("prefix not found: %s", prefix)
    }

    // Get basic stats
    stats := idx.Stats(pos)
    fmt.Printf("\n%s\n", prefix)
    fmt.Printf("  Objects: %d\n", stats.ObjectCount)
    fmt.Printf("  Bytes: %d\n", stats.TotalBytes)

    // Get tier breakdown
    if idx.HasTierData() {
        fmt.Println("\n  Tier breakdown:")
        for _, tb := range idx.TierBreakdown(pos) {
            fmt.Printf("    %s: %d objects, %d bytes\n",
                tb.TierName, tb.ObjectCount, tb.Bytes)
        }
    }

    // Find immediate children
    children, err := idx.DescendantsAtDepth(pos, 1)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("\n  Children (%d):\n", len(children))
    for _, childPos := range children {
        childPrefix, _ := idx.PrefixString(childPos)
        childStats := idx.Stats(childPos)
        fmt.Printf("    %s: %d objects\n", childPrefix, childStats.ObjectCount)
    }
}
```
