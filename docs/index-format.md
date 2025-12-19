# Index Format Specification

This document describes the on-disk format of s3-inv-db indexes.

## Directory Structure

```
index/
├── manifest.json           # Index metadata and checksums
├── prefixes.blob           # Prefix string data
├── prefixes.offsets        # Offset table for prefix lookup
├── mphf.bin                # Minimal perfect hash function
├── subtree_end.u64         # Subtree end positions
├── depth.u32               # Node depths
├── object_count.u64        # Object counts per prefix
├── total_bytes.u64         # Byte totals per prefix
├── max_depth_in_subtree.u32 # Maximum depth in each subtree
└── depth_index/            # Depth-based posting lists
    ├── depth_1.bin
    ├── depth_2.bin
    └── ...
```

## Manifest (`manifest.json`)

The manifest contains index metadata and checksums for integrity verification.

```json
{
  "version": 1,
  "node_count": 1234567,
  "max_depth": 15,
  "files": {
    "subtree_end.u64": {
      "size": 9876536,
      "xxhash64": "a1b2c3d4e5f67890"
    },
    "depth.u32": {
      "size": 4938268,
      "xxhash64": "b2c3d4e5f6789012"
    }
    // ... other files
  }
}
```

**Fields:**
- `version`: Format version (currently 1)
- `node_count`: Total number of prefix nodes
- `max_depth`: Maximum depth in the trie
- `files`: Map of filename to size and xxHash64 checksum

## Columnar Arrays

Each array file contains raw binary data with no headers. Values are stored in little-endian byte order.

### `subtree_end.u64`

Array of 64-bit unsigned integers. For node at position `i`, `subtree_end[i]` is the position of the last descendant in its subtree.

```
Position:    0    1    2    3    4    5
Value:       5    2    2    5    5    5
             │    │    │    │    │    │
             │    │    │    └────┴────┴── Leaf nodes (point to self)
             │    └────┴── Subtree ends at position 2
             └── Root's subtree ends at position 5
```

**Usage:** To check if position `j` is a descendant of position `i`:
```go
isDescendant := j > i && j <= subtreeEnd[i]
```

### `depth.u32`

Array of 32-bit unsigned integers. `depth[i]` is the depth of node at position `i`, where depth = number of "/" characters in the prefix.

```
Prefix          Depth
(root)          0
data/           1
data/2024/      2
data/2024/01/   3
```

### `object_count.u64`

Array of 64-bit unsigned integers. `object_count[i]` is the total number of objects under the prefix at position `i`, including all descendants.

### `total_bytes.u64`

Array of 64-bit unsigned integers. `total_bytes[i]` is the sum of all object sizes under the prefix at position `i`.

### `max_depth_in_subtree.u32`

Array of 32-bit unsigned integers. `max_depth_in_subtree[i]` is the maximum depth of any descendant in the subtree rooted at position `i`. Used to short-circuit depth queries.

## Prefix Dictionary

### `prefixes.blob`

Concatenated prefix strings with length prefixes:

```
┌────────────────────────────────────────────────┐
│ len₀ │ prefix₀ │ len₁ │ prefix₁ │ len₂ │ ... │
└────────────────────────────────────────────────┘
```

Each entry:
- 4 bytes: Length of prefix string (little-endian uint32)
- N bytes: UTF-8 prefix string (no null terminator)

### `prefixes.offsets`

Array of 64-bit offsets into `prefixes.blob`:

```
┌──────────────────────────────────────┐
│ offset₀ │ offset₁ │ offset₂ │ ... │
└──────────────────────────────────────┘
```

To read prefix at position `i`:
1. Read `offset = offsets[i]`
2. Seek to `offset` in blob
3. Read 4-byte length, then read that many bytes

### `mphf.bin`

Binary serialization of a BBHash minimal perfect hash function. Maps prefix strings to positions in O(1) time.

**Lookup algorithm:**
1. Compute `hash = MPHF(prefix)`
2. Read prefix at position `hash`
3. If prefix matches, return `hash`; otherwise, prefix doesn't exist

The MPHF guarantees no collisions for keys that exist in the index. For non-existent keys, the verification step catches false positives.

## Depth Index

The `depth_index/` directory contains posting lists for each depth level.

### `depth_N.bin`

Sorted array of positions at depth N:

```
┌─────────────────────────────────────────────┐
│ count │ pos₀ │ pos₁ │ pos₂ │ ... │ posₙ₋₁ │
└─────────────────────────────────────────────┘
```

- 8 bytes: Count of positions (little-endian uint64)
- N × 8 bytes: Sorted position array (little-endian uint64)

**Binary search usage:**
To find descendants of position `P` at relative depth `D`:
1. Load posting list for depth `depth[P] + D`
2. Binary search for positions in range `[P+1, subtreeEnd[P]]`

## Byte Order

All multi-byte integers are stored in **little-endian** byte order for efficient access on x86/x64 architectures.

## Memory Mapping

All files are designed for memory-mapped access:
- Fixed-size records enable direct indexing
- Page-aligned access patterns
- No parsing or deserialization required

## Checksums

All data files are checksummed with xxHash64 for integrity verification:
- Checksums computed during build
- Verified on index open (configurable)
- Detects corruption or incomplete writes

## Versioning

The format version in manifest.json enables future evolution:
- Version 1: Current format (documented here)
- Future versions may add fields or files
- Readers should reject unknown versions

## Size Estimates

For an index with N prefixes:

| File | Size |
|------|------|
| subtree_end.u64 | 8N bytes |
| depth.u32 | 4N bytes |
| object_count.u64 | 8N bytes |
| total_bytes.u64 | 8N bytes |
| max_depth_in_subtree.u32 | 4N bytes |
| mphf.bin | ~0.3N bytes |
| prefixes.blob | ~40N bytes (avg prefix length) |
| prefixes.offsets | 8N bytes |
| depth_index/ | ~8N bytes total |

**Total:** Approximately 80-90 bytes per prefix.

For 1 million prefixes: ~80-90 MB
For 100 million prefixes: ~8-9 GB
