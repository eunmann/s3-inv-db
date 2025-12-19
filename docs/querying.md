# Querying Indexes

This document explains how to query s3-inv-db indexes.

## Opening an Index

```go
import "github.com/eunmann/s3-inv-db/pkg/indexread"

idx, err := indexread.Open("./my-index")
if err != nil {
    log.Fatal(err)
}
defer idx.Close()
```

The `Open` function:
- Validates the manifest and checksums
- Memory-maps all data files
- Loads the MPHF and depth index

## Basic Operations

### Prefix Lookup

Look up a prefix to get its position in the index:

```go
pos, ok := idx.Lookup("data/2024/01/")
if !ok {
    fmt.Println("Prefix not found")
    return
}
fmt.Printf("Found at position %d\n", pos)
```

**Complexity:** O(1) - single MPHF hash + verification

**Note:** The prefix must match exactly, including trailing `/` for directories.

### Get Statistics

Retrieve aggregate statistics for a prefix:

```go
pos, ok := idx.Lookup("data/2024/")
if ok {
    stats := idx.Stats(pos)
    fmt.Printf("Objects: %d\n", stats.ObjectCount)
    fmt.Printf("Total bytes: %d\n", stats.TotalBytes)
}
```

The `Stats` struct contains:
- `ObjectCount`: Total objects under this prefix (including descendants)
- `TotalBytes`: Sum of all object sizes under this prefix

**Complexity:** O(1) - direct array access

### Combined Lookup + Stats

For convenience, lookup and stats in one call:

```go
stats, ok := idx.StatsForPrefix("data/2024/")
if ok {
    fmt.Printf("Objects: %d, Bytes: %d\n", stats.ObjectCount, stats.TotalBytes)
}
```

## Depth-Based Queries

### Get Descendants at Specific Depth

Find all prefixes at a relative depth from a starting prefix:

```go
// Get all prefixes at depth 1 below "data/"
pos, _ := idx.Lookup("data/")
children, err := idx.DescendantsAtDepth(pos, 1)
if err != nil {
    log.Fatal(err)
}

for _, childPos := range children {
    prefix, _ := idx.PrefixString(childPos)
    stats := idx.Stats(childPos)
    fmt.Printf("%s: %d objects, %d bytes\n", prefix, stats.ObjectCount, stats.TotalBytes)
}
```

**Parameters:**
- `prefixPos`: Position of the starting prefix
- `relDepth`: Relative depth (1 = immediate children, 2 = grandchildren, etc.)

**Returns:** Slice of positions at the target depth

**Complexity:** O(log N + K) where N = total nodes, K = result count

### Get Descendants Up To Depth

Get all prefixes from depth 1 to a maximum depth:

```go
// Get all prefixes up to 3 levels deep
pos, _ := idx.Lookup("data/")
depthLevels, err := idx.DescendantsUpToDepth(pos, 3)
if err != nil {
    log.Fatal(err)
}

for depth, positions := range depthLevels {
    fmt.Printf("Depth %d: %d prefixes\n", depth+1, len(positions))
}
```

**Returns:** Slice of slices, one per depth level (1-indexed in result)

### Filtered Descendants

Apply custom filters while querying:

```go
// Get prefixes with more than 1000 objects
filter := func(pos uint64) bool {
    stats := idx.Stats(pos)
    return stats.ObjectCount > 1000
}

pos, _ := idx.Lookup("data/")
filtered, err := idx.DescendantsAtDepthFiltered(pos, 1, filter)
```

**Use cases:**
- Filter by object count threshold
- Filter by total bytes
- Filter by prefix pattern (via PrefixString)

## Iterator Interface

For memory-efficient iteration over large result sets:

```go
pos, _ := idx.Lookup("data/")
iter, err := idx.NewDescendantIterator(pos, 1)
if err != nil {
    log.Fatal(err)
}

for iter.Next() {
    childPos := iter.Pos()
    prefix, _ := idx.PrefixString(childPos)
    stats := idx.Stats(childPos)

    fmt.Printf("%s: %d objects\n", prefix, stats.ObjectCount)
}
```

**Advantages:**
- No allocation of result slice
- Early termination possible
- Constant memory usage

## Node Properties

### Get Prefix String

Retrieve the full prefix string for a position:

```go
prefix, err := idx.PrefixString(pos)
// prefix = "data/2024/01/"
```

### Get Depth

Get the depth (number of `/` characters) of a prefix:

```go
depth := idx.Depth(pos)
// depth = 3 for "data/2024/01/"
```

### Get Subtree Range

Get the position range of all descendants:

```go
subtreeEnd := idx.SubtreeEnd(pos)
// All descendants are in range (pos, subtreeEnd]
```

### Check Maximum Depth in Subtree

Useful for short-circuiting deep queries:

```go
maxDepth := idx.MaxDepthInSubtree(pos)
// If maxDepth < currentDepth + targetRelDepth, no results exist
```

## Index Metadata

### Node Count

Total number of prefix nodes:

```go
count := idx.Count()
fmt.Printf("Index contains %d prefixes\n", count)
```

### Maximum Depth

Deepest level in the trie:

```go
maxDepth := idx.MaxDepth()
fmt.Printf("Maximum depth: %d\n", maxDepth)
```

## Query Patterns

### Top-N Prefixes by Size

```go
func topPrefixesByBytes(idx *indexread.Index, prefix string, n int) []string {
    pos, ok := idx.Lookup(prefix)
    if !ok {
        return nil
    }

    children, _ := idx.DescendantsAtDepth(pos, 1)

    // Sort by bytes descending
    sort.Slice(children, func(i, j int) bool {
        return idx.Stats(children[i]).TotalBytes > idx.Stats(children[j]).TotalBytes
    })

    result := make([]string, 0, n)
    for i := 0; i < n && i < len(children); i++ {
        prefix, _ := idx.PrefixString(children[i])
        result = append(result, prefix)
    }
    return result
}
```

### Recursive Prefix Exploration

```go
func explorePrefixes(idx *indexread.Index, pos uint64, depth int, indent string) {
    prefix, _ := idx.PrefixString(pos)
    stats := idx.Stats(pos)
    fmt.Printf("%s%s (%d objects, %s)\n", indent, prefix, stats.ObjectCount, humanBytes(stats.TotalBytes))

    if depth <= 0 {
        return
    }

    children, _ := idx.DescendantsAtDepth(pos, 1)
    for _, child := range children {
        explorePrefixes(idx, child, depth-1, indent+"  ")
    }
}
```

### Find Prefixes Matching Pattern

```go
func findPrefixesMatching(idx *indexread.Index, pattern *regexp.Regexp) []string {
    var matches []string

    for pos := uint64(0); pos < idx.Count(); pos++ {
        prefix, _ := idx.PrefixString(pos)
        if pattern.MatchString(prefix) {
            matches = append(matches, prefix)
        }
    }

    return matches
}
```

## Performance Characteristics

### Latency

| Operation | Latency (1M prefixes) |
|-----------|----------------------|
| Lookup | 5-6 μs |
| Stats | O(1), ~10 ns |
| DescendantsAtDepth(1) | 2-10 ms (depends on result count) |
| PrefixString | ~90 ns |
| Iterator.Next() | ~3 ns |

### Memory

- Index uses memory-mapped files
- Only accessed pages are resident in memory
- Multiple readers share OS page cache

### Concurrency

- All read operations are thread-safe
- No locks required for queries
- Safe to share `*Index` across goroutines

## Error Handling

Most errors occur during `Open()`:
- Invalid manifest
- Checksum mismatch
- Missing files

Query operations return errors for:
- Invalid positions (out of range)
- Depth index missing for requested depth

```go
children, err := idx.DescendantsAtDepth(pos, 100)
if err != nil {
    // Depth 100 doesn't exist in this index
    fmt.Printf("Error: %v\n", err)
}
```

## Best Practices

1. **Reuse the Index**: Open once, query many times
2. **Use iterators for large results**: Avoids allocation
3. **Check MaxDepthInSubtree**: Short-circuit impossible queries
4. **Batch related queries**: Leverage CPU cache locality
5. **Close when done**: Releases memory-mapped resources
