# Index Format

The index uses a row-major file format optimized for memory-mapped access. Each prefix has the same row index (its preorder position) in every per-prefix file, so `pos → all stats` is one row read in one mmap.

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
├── build.json                     (CLI builds only; not in the manifest)
└── tier_stats/
    ├── tier_stats_row.bin         (dense layout), or
    ├── tier_stats_sparse.bin      (sparse layout)
    └── tier_stats_sparse.off.u64
```

`tiers.json` and the `tier_stats/` files are absent from an index with
no prefixes.

Prefixes are stored in the dictionary-encoded layout described under
[Prefix Strings](#prefix-strings) below.

## File Header

Every binary file except `mph.bin` and `prefix_dict.bin` starts with
the same 20-byte header (the `Header` struct in `pkg/format`), all
fields little-endian:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Magic | `0x53334944` ("S3ID" in ASCII) |
| 4 | 4 | Version | Format version (currently `4`) |
| 8 | 8 | Count | Number of rows |
| 16 | 4 | Width | Row stride in bytes |

Data follows as a packed array of fixed-stride rows (`core_stats.bin`
puts a 5-byte stride descriptor first; see below).

### Variable-width arrays

The `.u64` and `.u32` suffixes name the type a reader decodes to, not
the stored width. At finalize the builder rewrites each of these
arrays at the smallest width in bytes that holds its largest value,
and records it in `Width`:

| File | Width |
|------|-------|
| `depth_positions.u64` | bytes for the largest position (1–8) |
| `depth_offsets.u64` | bytes for the number of positions (1–8) |
| `prefix_dict.prefix_off.u64` | bytes for the number of segment-ID references (1–8) |
| `prefix_dict.ids.u32` | bytes for the largest segment ID (1–4) |
| `tier_stats/tier_stats_sparse.off.u64` | bytes for the total sparse row length (1–8) |
| `prefix_dict.off.u64`, `mph_fp_pos.u64` | always 8 |

A reader loads 8 bytes (4 for `prefix_dict.ids.u32`) at
`index × Width` and masks off the bytes above `Width`. So that this
load stays inside the mmap at the last element, each repacked file
ends with a zero tail pad: 8 bytes for the `.u64` arrays, 4 bytes for
`prefix_dict.ids.u32`.

## Core Stats (`core_stats.bin`)

A single row-major file with one row per prefix at its preorder
position. Each field is stored at the smallest byte width that holds
its largest value in this index (`CoreStatsStride` in
`pkg/format/core_stats.go`), so the row width varies per index.

Layout:

| Offset | Size | Content |
|--------|------|---------|
| 0 | 20 | Header (`Width` = row width in bytes) |
| 20 | 5 | Stride descriptor: one byte per field, its width in bytes |
| 25 | `Count × Width` | Rows |
| end | 8 | Zero tail pad |

Fields, in row order:

| Field | Width (bytes) |
|-------|---------------|
| object_count | 1–8 |
| total_bytes | 1–8 |
| subtree_end | 1–8 |
| depth | 1–2 |
| max_depth_in_subtree | 1–2 |

A field's offset in the row is the sum of the widths before it. On
open the reader checks that the descriptor's widths sum to the
header's `Width`. A field read is one 8-byte little-endian load masked
to the field's width; the tail pad keeps that load inside the mmap on
the last row. On the 1M-object benchmark fixture the stride is
3 + 7 + 3 + 1 + 1 = 15 bytes, about 270 rows per 4 KiB page, so a
stats lookup is one page fault on a cold index and zero on a warm one.

During the build the rows are written at a fixed 28-byte stride
(8 + 8 + 8 + 2 + 2) into an mmap'd file. `object_count`, `total_bytes`,
and `depth` are written at `Add()` time; `subtree_end` and
`max_depth_in_subtree` are patched in place when the subtree closes.
At finalize the builder streams the rows into `core_stats.bin.tmp` at
the narrow stride and renames it over the build file.

### Preorder Positions

Prefixes are stored in preorder traversal order. A prefix's subtree
occupies contiguous positions `[pos, subtree_end]` — the **closed**
interval ending on the last descendant inclusive. Subtree iteration
is a range scan with no pointer chasing.

Position 0 is always the root prefix `""` (depth 0), whose subtree is
the whole index.

```
Position 0: ""              (subtree_end=6)
Position 1: data/           (subtree_end=4)
Position 2: data/2024/      (subtree_end=4)
Position 3: data/2024/01/   (subtree_end=3)
Position 4: data/2024/02/   (subtree_end=4)
Position 5: logs/           (subtree_end=6)
Position 6: logs/app/       (subtree_end=6)
```

To find all descendants of `data/` (position 1): iterate positions 2 through 4.

## Depth Index

Two files enable efficient "find all prefixes at depth N" queries:

| File | Description |
|------|-------------|
| `depth_offsets.u64` | `depth_offsets[d]` = starting index in `depth_positions` for depth `d`; a trailing sentinel holds the total |
| `depth_positions.u64` | Sorted positions of all prefixes at each depth |

To find prefixes at depth 2:
1. Read `depth_offsets[2]` and `depth_offsets[3]`
2. Slice `depth_positions[start:end]`
3. Binary search within a subtree range if needed

## MPHF (Minimal Perfect Hash Function)

The MPHF provides O(1) prefix-to-position lookups with BBHash (Limasset et al., 2017), implemented in `pkg/format/bbhash.go`.

| File | Description |
|------|-------------|
| `mph.bin` | BBHash levels: the level count, each level's length in 64-bit words, then every level's bit vector (all little-endian `uint64`) |
| `mph_fp_pos.u64` | Header (`Count` = 2 × prefixes, `Width` = 8), then interleaved (fingerprint, position) pairs — 16 bytes per slot, fingerprint first — then the 8-byte key seed |

The fingerprint+position pair is combined into one file so a lookup is a single mmap read instead of two.

### BBHash

Level 0 is a bit vector of about 2 bits per prefix (gamma 2). Each key hash sets the bit chosen by a splitmix64 mix of the hash and the level number. Bits that two keys share are cleared, and those keys move on to the next level, which is sized for them alone. A key's slot is the number of set bits before its own, across all levels. On load the reader builds a rank directory with one count per 512 bits, so that count takes at most eight popcounts. A lookup therefore costs O(levels) however many prefixes the index holds. `mph.bin` holds about 3.3 bits per prefix; with the in-memory rank directory the table is about 3.7 bits per prefix.

### Lookup Algorithm

1. Hash the prefix string using FNV-1a (64-bit) with the offset basis XORed with the key seed
2. Find the key hash's BBHash slot; a hash that sets no bit on any level is not in the index
3. Read the 16-byte slot at that index from `mph_fp_pos.u64`
4. Compute the FNV-1 fingerprint of the query prefix; if it matches the slot's, return the slot's position, otherwise the prefix doesn't exist

A non-member query is misreported only if it lands on an occupied slot and its 64-bit fingerprint also matches (~2⁻⁶⁴ per lookup). Comparing the stored prefix as well would make hits exact, but costs ~9% per lookup, so lookups stay fingerprint-only.

### Key Seed

BBHash needs distinct keys. If two prefixes share a 64-bit key hash, the builder rehashes every prefix with a new non-zero seed (up to 8 tries). Seed 0 is plain FNV-1a. The seed is always stored, even when it is 0, as one 8-byte word after the slots in `mph_fp_pos.u64`, and readers hash queries with it.

## Prefix Strings

Dictionary-encoded layout: each "/"-delimited segment is interned
once into a shared blob; each prefix is then stored as a sequence of
segment IDs. A prefix ending in `/` has a trailing empty segment, so
`data/2024/` is the three segments `data`, `2024` and `""`. Shared
top-level segments on a deep S3 hierarchy compress the prefix bytes
substantially.

| File | Description |
|------|-------------|
| `prefix_dict.bin` | Concatenated segment strings (no separators) |
| `prefix_dict.off.u64` | Byte offset for each segment in `prefix_dict.bin`; trailing sentinel marks the end |
| `prefix_dict.ids.u32` | Per-prefix concatenated segment-ID streams, at the width of the largest segment ID |
| `prefix_dict.prefix_off.u64` | Start offset (into `prefix_dict.ids.u32`) for each prefix; trailing sentinel marks the end |

To read prefix at position `i`:
1. Read `prefix_off[i]` and `prefix_off[i+1]` — the segment-ID range
2. For each ID in `ids[start:end]`, look up the segment string via `prefix_dict.bin` + `prefix_dict.off.u64`
3. Join with `/`

On Open, all segments are preloaded into an in-memory cache (unique
segments are few next to prefixes: 9,262 segments for 2.37M prefixes
on the 1M-object benchmark fixture), so the warm-path cost is one
cache lookup per segment rather than a blob read.

## Tier Statistics

The files under `tier_stats/` hold per-prefix `(count, bytes)` pairs,
one slot per tier listed in `tiers.json`. Slot order matches
`tiers.json` (sorted by tier ID).
The pipeline declares the present tiers (those with data anywhere in
the inventory) before the first row, so tiers with no data get no
slot. A builder that declares none records all 13 tiers.

Each slot is 16 bytes:

| Slot offset | Size | Field |
|-------------|------|-------|
| `0` | 8 | count (uint64) |
| `8` | 8 | bytes (uint64) |

To find tier `T`'s slot, look up its position in `tiers.json`.

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

An index holds exactly one of two layouts. The builder writes the
dense file first and, at finalize, converts it to the sparse pair when
that is smaller on disk.

### Dense (`tier_stats_row.bin`)

Header, then one row per prefix at its preorder position. A row is
`presentTiers × 16` bytes holding every slot, zeros included; the
stride is the header's `Width`. Readers check
`Width / 16 == len(tiers.json tiers)` on open and reject a mismatched
index rather than decode rows at the wrong offsets.

### Sparse (`tier_stats_sparse.bin` + `tier_stats_sparse.off.u64`)

`tier_stats_sparse.bin` is a header (`Count` = prefixes, `Width` = 0)
followed by variable-length rows:

| Size | Field |
|------|-------|
| 2 | Presence bitmap (uint16): bit `i` set ⇒ slot `i` is non-zero in this row |
| `16 × popcount(bitmap)` | The non-zero slots only, in slot order |

`tier_stats_sparse.off.u64` holds `Count + 1` byte offsets into the
row data (after the header): row `i` spans `[off[i], off[i+1])`. It is
a variable-width array (see above).

The builder picks sparse when
`2·N + 16·populated + (N+1)·offsetWidth < presentTiers·16·N`, where
`N` is the prefix count and `populated` the number of non-zero slots
across all rows. A single-tier index always stays dense. The reader
uses the sparse pair only when both files exist. The conversion
renames the offsets file into place first and the rows file last,
then deletes the dense file.

## Manifest

`manifest.json` lists every index file with its size and hex SHA-256
checksum, plus the prefix count and maximum depth. `verify` and
`format.VerifyManifest` check each file's size and checksum against it.

```json
{
  "created_at": "2026-01-02T03:04:05Z",
  "files": {
    "core_stats.bin": {"checksum": "9f2c…", "size": 35613798},
    "tier_stats/tier_stats_sparse.bin": {"checksum": "…", "size": 47241450},
    "...": {"checksum": "…", "size": 0}
  },
  "version": 1,
  "node_count": 2374251,
  "max_depth": 7
}
```

`version` is the manifest's own version (`ManifestVersion`), separate
from the file-header format version.

`tiers.json` lists the tiers that have a slot in the tier-stats rows
(`id`, `name` and `file` for each), in slot order.

## Versioning

The format version in file headers lets a reader refuse files it would mis-decode. Version 4 is the current format; readers accept only that version, and an index written by any other version is rebuilt.
