# Index Format

The index uses a row-major file format optimized for memory-mapped access. Each prefix occupies the same byte position in every file, so `pos → all stats` is a single seek into a single mmap.

## Directory Structure

```
index/
├── manifest.json
├── tiers.json
├── core_stats.bin
├── depth_offsets.u64
├── depth_positions.u64
├── mph.bin
├── mph_fp_pos.u64
├── prefix_blob.bin              ◀── present only when --prefix-dictionary=false
├── prefix_offsets.u64           ◀── present only when --prefix-dictionary=false
├── prefix_dict.bin              ◀── present when --prefix-dictionary=true (default)
├── prefix_dict.off.u64          ◀──    ″
├── prefix_dict.ids.u32          ◀──    ″
├── prefix_dict.prefix_off.u64   ◀──    ″
└── tier_stats/
    └── tier_stats_row.bin
```

Prefix storage has two on-disk shapes; exactly one is written per
build. See [Prefix Strings](#prefix-strings) below for the layout of
each.

## File Header

All `.u64` files share a common header (the per-file `Header` struct in `pkg/format`):

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Magic | `0x53334944` ("S3ID" in ASCII) |
| 4 | 4 | Version | Format version (currently `1`) |
| 8 | 8 | Count | Number of rows |
| 16 | 4 | Width | Row stride in bytes |

Data follows immediately as a packed array of fixed-stride rows.

## Core Stats (`core_stats.bin`)

A single row-major file that supersedes the five per-column files of the prior format. Each prefix gets one 28-byte row at its preorder position:

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | object_count (uint64) |
| 8 | 8 | total_bytes (uint64) |
| 16 | 8 | subtree_end (uint64) |
| 24 | 2 | depth (uint16) |
| 26 | 2 | max_depth_in_subtree (uint16) |

`object_count`, `total_bytes`, and `depth` are written at `Add()` time. `subtree_end` and `max_depth_in_subtree` are patched in place when the subtree closes (out-of-preorder writes against the same mmap region). A single mmap covers ~146 prefixes per 4 KiB page, so a stats lookup is one page fault on a cold index and zero on a warm one.

### Preorder Positions

Prefixes are stored in preorder traversal order. A prefix's descendants occupy contiguous positions `[pos, subtree_end)`, so subtree iteration is a range scan with no pointer chasing.

```
Position 0: data/           (subtree_end=5)
Position 1: data/2024/      (subtree_end=5)
Position 2: data/2024/01/   (subtree_end=4)
Position 3: data/2024/02/   (subtree_end=5)
Position 4: logs/           (subtree_end=6)
Position 5: logs/app/       (subtree_end=6)
```

To find all descendants of `data/` (position 0): iterate positions 1 through 4.

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
| `mph_fp_pos.u64` | Interleaved (fingerprint, position) pairs — 16 bytes per slot, fingerprint first |

The fingerprint+position pair is combined into one file so a lookup is a single mmap read instead of two.

### Lookup Algorithm

1. Hash the prefix string using FNV-1a (64-bit)
2. Query the BBHash to get a candidate index
3. Read the 16-byte slot at that index from `mph_fp_pos.u64`
4. Compute FNV-1 fingerprint of the query prefix
5. If fingerprints match, take the position from the same slot
6. If fingerprints don't match, the prefix doesn't exist

The two-hash design (FNV-1a for BBHash input, FNV-1 for verification) minimizes false positive collisions.

## Prefix Strings

Two layouts; the build flag `--prefix-dictionary` selects between
them. `OpenMPHF` auto-detects which is on disk and dispatches.

### Raw blob (`--prefix-dictionary=false`)

| File | Description |
|------|-------------|
| `prefix_blob.bin` | Concatenated prefix strings (no separators) |
| `prefix_offsets.u64` | Byte offset for each prefix in the blob |

To read prefix at position `i`:
1. Read `prefix_offsets[i]` and `prefix_offsets[i+1]`
2. Slice `prefix_blob[start:end]`

### Dictionary-encoded (default)

Each "/"-delimited segment is interned once into a shared blob; each
prefix is then stored as a sequence of `uint32` segment IDs. Shared
top-level segments on a deep S3 hierarchy can compress the
prefix bytes substantially.

| File | Description |
|------|-------------|
| `prefix_dict.bin` | Concatenated segment strings (no separators) |
| `prefix_dict.off.u64` | Byte offset for each segment in `prefix_dict.bin` |
| `prefix_dict.ids.u32` | Per-prefix concatenated segment-ID streams |
| `prefix_dict.prefix_off.u64` | Start offset (into `prefix_dict.ids.u32`) for each prefix; trailing sentinel marks the end |

To read prefix at position `i`:
1. Read `prefix_off[i]` and `prefix_off[i+1]` — the segment-ID range
2. For each ID in `ids[start:end]`, look up the segment string via `prefix_dict.bin` + `prefix_dict.off.u64`
3. Join with `/`

On Open, all segments are preloaded into an in-memory cache (typical
inventories have ~100s of unique segments), so the warm-path cost is
one map lookup per segment rather than a blob read.

## Tier Statistics (`tier_stats/tier_stats_row.bin`)

A single row-major file that supersedes the per-tier-per-metric file pairs of the prior format. Each prefix gets one fixed-stride row covering every tier slot in tier-ID order, regardless of which tiers have data for that prefix.

Stride = `NumTiers × 16` bytes (8 for count, 8 for bytes, per tier). Slot for tier `T` is at offset `T*16` within the row:

| Slot offset | Size | Field |
|-------------|------|-------|
| `T*16 + 0` | 8 | count for tier `T` |
| `T*16 + 8` | 8 | bytes for tier `T` |

Tier IDs (0–12) map to S3 storage classes:

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
| 12 | INTELLIGENT_TIERING (Frequent, < 128 KiB) |

The fixed stride trades disk space for branch-free per-prefix reads — empty slots are written as zeros rather than omitted. The `tiers.json` manifest records which tier IDs actually have data so callers can skip rendering empty columns.

## Manifest

`manifest.json` contains file checksums for integrity verification:

```json
{
  "files": {
    "core_stats.bin": "sha256:...",
    "depth_positions.u64": "sha256:...",
    "...": "..."
  }
}
```

`tiers.json` records which tier IDs have non-zero data in this index.

## Versioning

The format version in file headers enables forward compatibility. Version 1 is the current format. Future versions may add new files or extend headers while maintaining backward-compatible readers.
