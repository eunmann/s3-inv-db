# Querying Indexes

Query s3-inv-db indexes for prefix statistics with O(1) lookups and efficient depth-based traversal.

## Opening an Index

```go
import "github.com/eunmann/s3-inv-db/pkg/indexread"

idx, err := indexread.Open("./my-index")
if err != nil {
    log.Fatal(err)
}
defer idx.Close()
```

The `Open` function validates checksums and memory-maps all data files.

## Basic Operations

### Lookup

```go
pos, ok := idx.Lookup("data/2024/01/")
if !ok {
    fmt.Println("Prefix not found")
    return
}
```

**Complexity:** O(1) - MPHF hash + fingerprint verification

Prefixes must match exactly, including trailing `/`.

### Get Statistics

```go
stats := idx.Stats(pos)
fmt.Printf("Objects: %d, Bytes: %d\n", stats.ObjectCount, stats.TotalBytes)
```

**Complexity:** O(1) - direct array access

### Combined Lookup + Stats

```go
stats, ok := idx.StatsForPrefix("data/2024/")
```

## Depth-Based Queries

### Descendants at Depth

```go
pos, _ := idx.Lookup("data/")
children, err := idx.DescendantsAtDepth(pos, 1)  // Immediate children

for _, childPos := range children {
    prefix, _ := idx.PrefixString(childPos)
    stats := idx.Stats(childPos)
    fmt.Printf("%s: %d objects\n", prefix, stats.ObjectCount)
}
```

**Complexity:** O(log N + K) where N = total nodes, K = result count

### Descendants Up To Depth

```go
depthLevels, err := idx.DescendantsUpToDepth(pos, 3)
for depth, positions := range depthLevels {
    fmt.Printf("Depth %d: %d prefixes\n", depth+1, len(positions))
}
```

### Filtered Queries

```go
filter := indexread.Filter{
    MinCount: 1000,     // At least 1000 objects
    MinBytes: 1 << 30,  // At least 1 GB
}
filtered, err := idx.DescendantsAtDepthFiltered(pos, 1, filter)
```

## Iterator Interface

For memory-efficient iteration:

```go
iter, err := idx.NewDescendantIterator(pos, 1)
for iter.Next() {
    prefix, _ := idx.PrefixString(iter.Pos())
    // Process...
}
```

## Node Properties

| Method | Returns |
|--------|---------|
| `PrefixString(pos)` | Full prefix string |
| `Depth(pos)` | Number of `/` in prefix |
| `SubtreeEnd(pos)` | Last descendant position |
| `MaxDepthInSubtree(pos)` | Deepest level in subtree |

## Tier Statistics

Tier data is available automatically if the index was built from inventory with `StorageClass`.

```go
if idx.HasTierData() {
    breakdown := idx.TierBreakdown(pos)
    for _, tb := range breakdown {
        fmt.Printf("%s: %d objects, %d bytes\n",
            tb.TierName, tb.ObjectCount, tb.Bytes)
    }
}
```

### Cost Estimation

```go
import "github.com/eunmann/s3-inv-db/pkg/pricing"

breakdown := idx.TierBreakdown(pos)
prices := pricing.DefaultUSEast1Prices()
cost := pricing.ComputeMonthlyCost(breakdown, prices)
fmt.Printf("Monthly cost: %s\n", pricing.FormatCost(cost.TotalMicrodollars))
```

## CLI Query

```bash
# Basic query
s3inv-index query --index ./my-index --prefix "data/2024/"

# With tier breakdown
s3inv-index query --index ./my-index --prefix "data/2024/" --show-tiers

# With cost estimation
s3inv-index query --index ./my-index --prefix "data/" --estimate-cost

# Custom price table
s3inv-index query --index ./my-index --prefix "data/" --estimate-cost --price-table ./prices.json
```

## Performance

| Operation | Latency |
|-----------|---------|
| Lookup | ~5 μs |
| Stats | ~10 ns |
| PrefixString | ~90 ns |
| Iterator.Next() | ~3 ns |
| DescendantsAtDepth(1) | 2-10 ms |

- Memory-mapped files; only accessed pages are resident
- All read operations are thread-safe and lock-free
- Safe to share `*Index` across goroutines
