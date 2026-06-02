# Index Format

The index uses a row-major file format optimized for memory-mapped access. Each prefix occupies the same byte position in every file, so `pos → all stats` is a single seek into a single mmap.

## What's in the index — and what isn't

The index is **prefix-aggregated only**. The smallest unit stored
anywhere is a prefix; there is no per-object record. Each row in
`core_stats.bin` carries the *sum* of `object_count` and `total_bytes`
over every S3 object whose key sits under that prefix, and the
per-tier file does the same per storage class.

What this **rules out** without re-reading the source inventory:

- The size or storage class of a specific object key.
- "Largest 100 objects under this prefix" / per-object distributions.
- Distinguishing a leaf-object key from a deeper prefix in the tree.

This is a deliberate trade — keeping per-object rows would multiply
on-disk size by orders of magnitude. UI and CLI surfaces should
reinforce the prefix-tree mental model (text labels like "child
prefix", "Browse", "Compare") and **avoid file-system or document
metaphors** (folder/file/paper icons, the word "sub-folder") that
imply per-object operations are possible.

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
├── prefix_dict.bin
├── prefix_dict.off.u64
├── prefix_dict.ids.u32
├── prefix_dict.prefix_off.u64
└── tier_stats/
    └── tier_stats_row.bin
```

Prefixes are stored in the dictionary-encoded layout described under
[Prefix Strings](#prefix-strings) below.

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

Prefixes are stored in preorder traversal order. A prefix's subtree
occupies contiguous positions `[pos, subtree_end]` — the **closed**
interval ending on the last descendant inclusive. Subtree iteration
is a range scan with no pointer chasing.

```
Position 0: data/           (subtree_end=3)
Position 1: data/2024/      (subtree_end=3)
Position 2: data/2024/01/   (subtree_end=2)
Position 3: data/2024/02/   (subtree_end=3)
Position 4: logs/           (subtree_end=5)
Position 5: logs/app/       (subtree_end=5)
```

To find all descendants of `data/` (position 0): iterate positions 1 through 3.

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

Dictionary-encoded layout: each "/"-delimited segment is interned
once into a shared blob; each prefix is then stored as a sequence of
`uint32` segment IDs. Shared top-level segments on a deep S3
hierarchy compress the prefix bytes substantially.

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

A single row-major file holding per-prefix `(count, bytes)` slots. Slot order matches `tiers.json` (sorted by tier ID); the on-disk stride is `presentTiers × 16` bytes, where `presentTiers` is the number of tier IDs that have non-zero data anywhere in the index.

Layout per row (variable stride):

| Slot offset | Size | Field |
|-------------|------|-------|
| `i*16 + 0` | 8 | count for the i-th manifest tier |
| `i*16 + 8` | 8 | bytes for the i-th manifest tier |

To find tier `T`'s slot in a row, look up its position in `tiers.json`. Tier IDs absent from the manifest contributed no data in this index and are not stored.

Tier IDs (0–12) map to S3 storage classes:

| ID | S3 name | Notes |
|----|---|---|
| 0 | `STANDARD` |  |
| 1 | `STANDARD_IA` | 128 KiB minimum billable size |
| 2 | `ONEZONE_IA` | 128 KiB minimum billable size |
| 3 | `GLACIER_IR` | 128 KiB minimum billable size |
| 4 | `GLACIER` | Per-object metadata overhead |
| 5 | `DEEP_ARCHIVE` | Per-object metadata overhead |
| 6 | `REDUCED_REDUNDANCY` | Deprecated by AWS |
| 7 | `INTELLIGENT_TIERING_FREQUENT` | Monitored |
| 8 | `INTELLIGENT_TIERING_INFREQUENT` | Monitored |
| 9 | `INTELLIGENT_TIERING_ARCHIVE_INSTANT` | Monitored |
| 10 | `INTELLIGENT_TIERING_ARCHIVE` | Monitored + Glacier overhead |
| 11 | `INTELLIGENT_TIERING_DEEP_ARCHIVE` | Monitored + Glacier overhead |
| 12 | `INTELLIGENT_TIERING_FREQUENT_SMALL` | Synthetic bucket for IT-Frequent objects < 128 KiB; billed at Frequent rate, excluded from the monitoring fee |

`pkg/tiers.Resolve(id, size)` re-routes IT-Frequent objects below
128 KiB into the synthetic `ITFrequentSmall` bucket at ingest time so
cost estimates honour the AWS minimum-monitored-size rule exactly.

### Build-time layout

During ingest the writer emits a dense intermediate with `NumTiers × 16` byte stride in tier-ID order (so the writer doesn't need to know the present-tier set until all rows are seen). At finalize, `PackTierStatsRow` rewrites the file to the packed stride above, dropping slots for tiers with zero data globally. The pack writes to `tier_stats_row.bin.tmp` + `rename`, then fsyncs the `tier_stats/` directory.

The stride is recorded in the file header's `Width` field; readers cross-check `Width / 16 == len(manifest.Tiers)` on open and reject mismatched indexes rather than risk decoding rows at the wrong offsets.

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
