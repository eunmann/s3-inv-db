# Index Format

The index uses a columnar file format optimized for memory-mapped access.

## Directory Structure

```
index/
├── manifest.json
├── subtree_end.u64
├── depth.u32
├── object_count.u64
├── total_bytes.u64
├── max_depth_in_subtree.u32
├── depth_offsets.u64
├── depth_positions.u64
├── mph.bin
├── mph_fp.u64
├── mph_pos.u64
├── prefix_blob.bin
├── prefix_offsets.u64
└── tier_stats/
    ├── tier_0_count.u64
    ├── tier_0_bytes.u64
    ├── tier_1_count.u64
    ├── tier_1_bytes.u64
    └── ...
```

## File Header

All `.u32` and `.u64` files share a common 20-byte header:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Magic | `0x53334944` ("S3ID" in ASCII) |
| 4 | 4 | Version | Format version (currently `1`) |
| 8 | 8 | Count | Number of elements |
| 16 | 4 | Width | Element size in bytes (4 or 8) |

Data follows immediately after the header as a packed array of little-endian integers.

## Core Arrays

These files store per-prefix statistics. Element `i` corresponds to the prefix at preorder position `i`.

| File | Type | Description |
|------|------|-------------|
| `subtree_end.u64` | uint64 | Exclusive end position of subtree rooted at this prefix |
| `depth.u32` | uint32 | Directory depth (number of `/` characters) |
| `object_count.u64` | uint64 | Total objects under this prefix |
| `total_bytes.u64` | uint64 | Total bytes under this prefix |
| `max_depth_in_subtree.u32` | uint32 | Maximum depth of any descendant |

### Preorder Positions

Prefixes are stored in preorder traversal order. This means a prefix's descendants occupy contiguous positions `[pos, subtree_end)`. This enables efficient subtree iteration without pointer chasing.

Example tree:
```
Position 0: data/           (subtree_end=5)
Position 1: data/2024/      (subtree_end=5)
Position 2: data/2024/01/   (subtree_end=4)
Position 3: data/2024/02/   (subtree_end=5)
Position 4: logs/           (subtree_end=6)
Position 5: logs/app/       (subtree_end=6)
```

To find all descendants of `data/` (position 0), iterate positions 1 through 4.

## Depth Index

Two files enable efficient "find all prefixes at depth N" queries:

| File | Description |
|------|-------------|
| `depth_offsets.u64` | `depth_offsets[d]` = starting index in `depth_positions` for depth `d` |
| `depth_positions.u64` | Sorted positions of all prefixes at each depth |

To find prefixes at depth 2:
1. Read `depth_offsets[2]` and `depth_offsets[3]`
2. Slice `depth_positions[start:end]`
3. Binary search within a subtree range if needed

## MPHF (Minimal Perfect Hash Function)

The MPHF provides O(1) prefix-to-position lookups using [BBHash](https://github.com/rizkg/BBHash).

| File | Description |
|------|-------------|
| `mph.bin` | Serialized BBHash structure |
| `mph_fp.u64` | FNV-1 fingerprints for verification |
| `mph_pos.u64` | Actual positions (MPHF output → real position) |

### Lookup Algorithm

1. Hash the prefix string using FNV-1a (64-bit)
2. Query the BBHash to get a candidate index
3. Read the fingerprint at that index from `mph_fp.u64`
4. Compute FNV-1 fingerprint of the query prefix
5. If fingerprints match, read the position from `mph_pos.u64`
6. If fingerprints don't match, the prefix doesn't exist

The two-hash design (FNV-1a for BBHash input, FNV-1 for verification) minimizes false positive collisions.

## Prefix Strings

| File | Description |
|------|-------------|
| `prefix_blob.bin` | Concatenated prefix strings (no separators) |
| `prefix_offsets.u64` | Byte offset for each prefix in the blob |

To read prefix at position `i`:
1. Read `prefix_offsets[i]` and `prefix_offsets[i+1]`
2. Slice `prefix_blob[start:end]`

## Tier Statistics

Per-tier storage breakdowns are stored in `tier_stats/`:

| File | Description |
|------|-------------|
| `tier_N_count.u64` | Object count in tier N per prefix |
| `tier_N_bytes.u64` | Byte count in tier N per prefix |

Tier IDs (0-11) map to S3 storage classes:

| ID | Storage Class |
|----|---------------|
| 0 | STANDARD |
| 1 | STANDARD_IA |
| 2 | ONEZONE_IA |
| 3 | GLACIER_IR |
| 4 | GLACIER |
| 5 | DEEP_ARCHIVE |
| 6 | REDUCED_REDUNDANCY |
| 7 | INTELLIGENT_TIERING (Frequent) |
| 8 | INTELLIGENT_TIERING (Infrequent) |
| 9 | INTELLIGENT_TIERING (Archive Instant) |
| 10 | INTELLIGENT_TIERING (Archive) |
| 11 | INTELLIGENT_TIERING (Deep Archive) |

Tier files are only created for tiers with non-zero data.

## Manifest

`manifest.json` contains file checksums for integrity verification:

```json
{
  "files": {
    "subtree_end.u64": "sha256:...",
    "depth.u32": "sha256:...",
    ...
  }
}
```

## Versioning

The format version in file headers enables forward compatibility. Version 1 is the current format. Future versions may add new files or extend headers while maintaining backward-compatible readers.
