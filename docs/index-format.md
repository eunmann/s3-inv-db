# Index Format Specification

This document describes the on-disk format of s3-inv-db indexes.

## Directory Structure

```
index/
├── manifest.json            # Metadata and checksums
├── subtree_end.u64          # Subtree end positions
├── depth.u32                # Node depths
├── object_count.u64         # Object counts per prefix
├── total_bytes.u64          # Byte totals per prefix
├── max_depth_in_subtree.u32 # Maximum depth in each subtree
├── depth_offsets.u64        # Depth index offsets
├── depth_positions.u64      # Positions sorted by depth
├── mph.bin                  # BBHash MPHF
├── mph_fp.u64               # Fingerprints for verification
├── mph_pos.u64              # Hash position → preorder position
├── prefix_blob.bin          # Concatenated prefix strings
├── prefix_offsets.u64       # Offsets into prefix blob
├── tiers.json               # Tier manifest (if present)
└── tier_stats/              # Per-tier statistics (if present)
    ├── standard_bytes.u64
    ├── standard_count.u64
    └── ...
```

## Manifest

`manifest.json` contains index metadata and SHA-256 checksums:

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

## Columnar Array Format

All `.u64` and `.u32` files share a common header:

```
┌─────────────────────────────────────────┐
│ Magic (4B)  │ Version (4B) │ Count (8B) │
│ Width (4B)  │ Data...                   │
└─────────────────────────────────────────┘
```

- **Magic**: `0x53334944` ("S3ID")
- **Version**: Format version (1)
- **Count**: Number of elements
- **Width**: Element size in bytes (4 or 8)
- **Data**: Little-endian values

### Core Arrays

| File | Type | Description |
|------|------|-------------|
| `subtree_end.u64` | uint64 | Position of last descendant for node `i` |
| `depth.u32` | uint32 | Depth (number of `/` in prefix) |
| `object_count.u64` | uint64 | Total objects in subtree |
| `total_bytes.u64` | uint64 | Total bytes in subtree |
| `max_depth_in_subtree.u32` | uint32 | Max depth of any descendant |

**Subtree check**: Position `j` is a descendant of `i` if `j > i && j <= subtree_end[i]`.

## Depth Index

Enables queries like "all prefixes at depth N within subtree".

### Files

- `depth_offsets.u64`: Array of `max_depth + 2` offsets into positions array
- `depth_positions.u64`: All positions sorted by depth, then by position

### Usage

```go
// Positions at depth d
startIdx := depth_offsets[d]
endIdx := depth_offsets[d+1]
positions := depth_positions[startIdx:endIdx]

// Binary search to find positions in subtree range
```

## MPHF (Minimal Perfect Hash Function)

Maps prefix strings to positions using [BBHash](https://github.com/relab/bbhash).

### Files

| File | Description |
|------|-------------|
| `mph.bin` | Serialized BBHash (~2.5 bits/key) |
| `mph_fp.u64` | Fingerprints indexed by hash position |
| `mph_pos.u64` | Hash position → preorder position mapping |

### Hash Functions

Two different FNV variants reduce collision probability:

- **Key hash** (FNV-1a): Used for MPHF construction and lookup
- **Fingerprint** (FNV-1): Used for verification

### Lookup Algorithm

```go
func Lookup(prefix string) (pos uint64, ok bool) {
    // 1. Hash prefix for MPHF lookup (FNV-1a)
    keyHash := fnv1a(prefix)

    // 2. Get candidate position from MPHF
    hashPos := mph.Find(keyHash) - 1  // BBHash returns 1-indexed

    // 3. Verify with fingerprint (FNV-1)
    if mph_fp[hashPos] != fnv1(prefix) {
        return 0, false  // Not in index
    }

    // 4. Return preorder position
    return mph_pos[hashPos], true
}
```

## Prefix Strings

### Files

- `prefix_blob.bin`: Concatenated UTF-8 strings (no delimiters)
- `prefix_offsets.u64`: N+1 offsets; prefix `i` spans `[offsets[i], offsets[i+1])`

### Example

```
prefix_blob.bin:  "a/a/b/a/b/c/data/"
prefix_offsets:   [0, 2, 6, 12, 17]

Position 0: bytes[0:2]   = "a/"
Position 1: bytes[2:6]   = "a/b/"
Position 2: bytes[6:12]  = "a/b/c/"
Position 3: bytes[12:17] = "data/"
```

## Tier Statistics (Optional)

Created automatically when inventory includes `StorageClass` column.

### Files

- `tiers.json`: Tier manifest listing present tiers
- `tier_stats/<tier>_bytes.u64`: Bytes per node for tier
- `tier_stats/<tier>_count.u64`: Object count per node for tier

### Tier Manifest

```json
{
  "tiers": [0, 1, 5],
  "names": ["STANDARD", "STANDARD_IA", "GLACIER"]
}
```

Tier IDs map to S3 storage classes. Only tiers with data are included.

## Size Estimates

For N prefixes:

| Component | Size |
|-----------|------|
| Core arrays (5 files) | ~32N bytes |
| Depth index | ~16N bytes |
| MPHF + fingerprints + positions | ~18N bytes |
| Prefix strings | ~50N bytes (varies) |
| Tier stats (optional) | ~16N bytes per tier |

**Typical total**: ~120 bytes per prefix (without tiers)

| Prefix Count | Index Size |
|-------------:|----------:|
| 100,000 | ~12 MB |
| 1,000,000 | ~120 MB |
| 10,000,000 | ~1.2 GB |
