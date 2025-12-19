# Index Format Specification

This document describes the on-disk format of s3-inv-db indexes.

## Directory Structure

```
index/
├── manifest.json            # Index metadata and checksums
├── subtree_end.u64          # Subtree end positions
├── depth.u32                # Node depths
├── object_count.u64         # Object counts per prefix
├── total_bytes.u64          # Byte totals per prefix
├── max_depth_in_subtree.u32 # Maximum depth in each subtree
├── depth_offsets.u64        # Depth index: offsets into positions array
├── depth_positions.u64      # Depth index: sorted positions by depth
├── mph.bin                  # Minimal perfect hash function
├── mph_fp.u64               # MPHF fingerprints for verification
├── mph_pos.u64              # MPHF hash position to preorder position
├── prefix_blob.bin          # Concatenated prefix strings
├── prefix_offsets.u64       # Offsets into prefix blob
├── tiers.json               # Tier manifest (if tier tracking enabled)
└── tier_stats/              # Per-tier statistics (if tier tracking enabled)
    ├── tier_0_bytes.u64
    ├── tier_0_counts.u64
    └── ...
```

## Manifest (`manifest.json`)

The manifest contains index metadata and SHA-256 checksums for integrity verification.

```json
{
  "version": 1,
  "created_at": "2024-01-15T10:30:00Z",
  "node_count": 1234567,
  "max_depth": 15,
  "files": {
    "subtree_end.u64": {
      "size": 9876536,
      "checksum": "a1b2c3d4..."
    }
  }
}
```

**Fields:**
- `version`: Format version (currently 1)
- `created_at`: Build timestamp
- `node_count`: Total number of prefix nodes
- `max_depth`: Maximum depth in the trie
- `files`: Map of filename to size and SHA-256 checksum

## Columnar Array Format

All columnar array files share a common header format:

```
┌──────────────────────────────────────────────┐
│ Magic (4 bytes) │ Version (4 bytes) │        │
│ Count (8 bytes) │ Width (4 bytes)   │ Data   │
└──────────────────────────────────────────────┘
```

- **Magic**: `0x53334944` ("S3ID")
- **Version**: Format version (1)
- **Count**: Number of elements
- **Width**: Element size in bytes (4, 8, etc.)
- **Data**: Raw values in little-endian byte order

### `subtree_end.u64`

For node at position `i`, `subtree_end[i]` is the position of the last descendant.

**Usage:** To check if position `j` is a descendant of position `i`:
```go
isDescendant := j > i && j <= subtreeEnd[i]
```

### `depth.u32`

Node depth = number of "/" characters in the prefix.

### `object_count.u64` / `total_bytes.u64`

Aggregated counts including all descendants in the subtree.

### `max_depth_in_subtree.u32`

Maximum depth of any descendant. Used to short-circuit depth queries.

## Depth Index

The depth index enables efficient queries like "all prefixes at depth N".

### `depth_offsets.u64`

Array of offsets into `depth_positions.u64`, one per depth level plus a sentinel.

```
depth_offsets[d] = start index of depth d in positions array
depth_offsets[d+1] = end index (exclusive)
```

### `depth_positions.u64`

Sorted array of all positions, grouped by depth. Within each depth level,
positions are sorted ascending (which corresponds to alphabetical prefix order).

**Binary search usage:**
```go
// Find positions at depth d in subtree [start, end]
startIdx := depth_offsets[d]
endIdx := depth_offsets[d+1]
positions := depth_positions[startIdx:endIdx]
// Binary search for positions in range
```

## MPHF (Minimal Perfect Hash Function)

### `mph.bin`

Serialized BBHash minimal perfect hash function. Maps prefix strings to
consecutive integers [0, N) with no collisions for keys in the index.

### `mph_fp.u64`

Fingerprints (FNV-1a hashes) of each prefix, indexed by MPHF output position.
Used to verify lookups and reject non-existent prefixes.

### `mph_pos.u64`

Maps MPHF output position to preorder trie position. Needed because MPHF
assigns arbitrary positions, but we need the actual trie position.

**Lookup algorithm:**
1. `h = MPHF(prefix)` - get hash position
2. `fp = FNV1a(prefix)` - compute fingerprint
3. If `mph_fp[h] != fp` → prefix not found
4. `pos = mph_pos[h]` - get preorder position
5. Return `pos`

## Prefix Strings

### `prefix_blob.bin`

Concatenated UTF-8 prefix strings with no delimiters:
```
"a/"  "a/b/"  "a/b/c/"  "data/"  ...
```

### `prefix_offsets.u64`

Array of N+1 offsets. Prefix at position `i` spans bytes `[offsets[i], offsets[i+1])`.

## Tier Statistics (Optional)

Created when building with `--track-tiers`.

### `tiers.json`

Lists which tiers are present in the index:
```json
{
  "tiers": [0, 1, 5],
  "names": ["STANDARD", "STANDARD_IA", "GLACIER"]
}
```

### `tier_stats/tier_N_bytes.u64`

Per-node byte count for tier N. Only created for tiers with data.

### `tier_stats/tier_N_counts.u64`

Per-node object count for tier N.

## Size Estimates

For an index with N prefixes:

| Component | Size |
|-----------|------|
| Core arrays (5 files) | ~32N bytes |
| Depth index | ~16N bytes |
| MPHF + mappings | ~18N bytes |
| Prefix strings | ~50N bytes (varies) |
| Tier stats (optional) | ~16N bytes per tier |

**Total (without tiers):** ~120 bytes per prefix
**Total (with tiers):** ~120 + 16T bytes per prefix (T = number of tiers)

For 1 million prefixes: ~120 MB (without tiers)
