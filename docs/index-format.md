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
├── core_patch.bin                 (only when patching pays)
├── core_patch.rows
├── depth_offsets.u64
├── depth_positions.u64
├── mph.bin
├── mph_slots.bin
├── prefix_dict.bin
├── prefix_dict.off.u64
├── prefix_tree.bin
├── build.json                     (CLI builds only; not in the manifest)
└── tier_stats/                    (absent with one present tier)
    ├── tier_dense.bin             (dense layout), or
    ├── tier_multi.bin             (multi layout)
    ├── tier_multi.rows
    └── tier_multi.off.u64
```

`tiers.json` and the `tier_stats/` files are absent from an index with
no prefixes.

Each prefix is stored in `prefix_tree.bin` as its parent plus one
dictionary segment, described under [Prefix Strings](#prefix-strings)
below.

## File Header

Every binary file except `mph.bin` and `prefix_dict.bin` starts with
the same 20-byte header (the `Header` struct in `pkg/format`), all
fields little-endian:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | Magic | `0x53334944` ("S3ID" in ASCII) |
| 4 | 4 | Version | Format version (currently `9`) |
| 8 | 8 | Count | Number of rows |
| 16 | 4 | Width | Row stride in bytes |

Data follows as a packed array of fixed-stride rows (`core_stats.bin`,
`tier_dense.bin` and `tier_multi.bin` put a field-width descriptor
first; see below).

### Variable-width arrays

The `.u64` and `.u32` suffixes name the type a reader decodes to, not
the stored width. At finalize the builder rewrites each of these
arrays at the smallest width in bytes that holds its largest value,
and records it in `Width`:

| File | Width |
|------|-------|
| `depth_positions.u64` | bytes for the largest position (1–8) |
| `depth_offsets.u64` | bytes for the number of positions (1–8) |
| `tier_stats/tier_multi.off.u64` | bytes for the number of stored tier slots (1–8) |
| `prefix_dict.off.u64` | bytes for the segment blob's size (1–8) |

A reader loads 8 bytes at `index × Width` and masks off the bytes
above `Width`. So that this load stays inside the mmap at the last
element, each repacked file ends with an 8-byte zero tail pad.

## Core Stats (`core_stats.bin`)

A single row-major file with one row per prefix at its preorder
position, plus, when it pays, two patch files for the few values that
don't fit their field (`pkg/format/core_stats.go`, `core_patch.go`).

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
| object_count | 1–8, patched |
| total_bytes | 1–8, patched |
| subtree_size | 1–8, patched: `subtree_end − pos` |
| depth | 1–2 |
| tier_mask | 0–2 |

A field's offset in the row is the sum of the widths before it. On
open the reader checks that the descriptor's widths sum to the
header's `Width`. Rows are packed with no padding: aligning them to
cache lines measured no faster. `tier_mask` bit *i* is set when
tier slot *i* (the *i*-th tier in `tiers.json`) holds data in this
prefix. Its width is 0 when no prefix has data outside slot 0; the mask
is then slot 0 for a prefix with data. A prefix has children exactly
when `subtree_size > 0`.

A field read is one 8-byte little-endian load masked to the field's
width; the tail pad keeps that load inside the mmap on the last row.

### Patched fields

The root and the top levels carry bucket-wide totals and subtrees, so
a field sized for its largest value would make every row pay for a
handful. Instead the builder picks each patched field's width from the
data:

- At finalize it reads every row once and counts rows by the widths
  their three patched values need, then sizes every width combination
  exactly (rows × widths, plus the patch files for the rows that don't
  fit) and keeps the smallest whose exceptions are at most 0.5% of
  rows. Exceptions cost a rank and a table read and sit on the upper
  levels that listings read most, so the cap keeps reads fast (#54).
  When no narrower width pays for its exceptions it uses plain widths
  and writes no patch files.
- A value that doesn't fit stores the field's all-ones sentinel (a
  value equal to it needs the next width). Its row's bit is set in
  `core_patch.rows`, a ranked bit vector (layout as in
  `tier_multi.rows`), and the row's exact `object_count`,
  `total_bytes` and `subtree_size` go to `core_patch.bin` (Header,
  `Width` = 24, then three `uint64`s per exception in row order).
- A reader that loads a sentinel from a patched field (one under 8
  bytes in an index with patch files) reads the exact value at
  `rank(pos)` in `core_patch.bin`; every other read is the plain load.

On the 1M-object benchmark fixture the stride is 1 + 5 + 1 + 1 + 2 = 10
bytes with 0.04% of rows patched, against 3 + 7 + 3 + 1 + 2 = 16 bytes
unpatched.

During the build the rows are written at a fixed 28-byte stride
(8 + 8 + 8 + 2 + 2) into an mmap'd file. `object_count`, `total_bytes`,
`depth` and `tier_mask` are written at `Add()` time; `subtree_size` is
written in place when the subtree closes. At finalize the builder
streams the rows into `core_stats.bin.tmp` at the chosen stride and
renames it over the build file.

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
| `mph_slots.bin` | Per BBHash slot, the prefix's fingerprint and its preorder position |

`mph_slots.bin` layout:

| Offset | Size | Content |
|--------|------|---------|
| 0 | 20 | Header (`Count` = slots, `Width` = 8 + position width) |
| 20 | 1 | Position width in bytes (the bytes the largest position needs) |
| 21 | `Count × Width` | Slots: 8-byte fingerprint, then the position |
| end − 16 | 8 | Key seed |
| end − 8 | 8 | Zero tail pad |

The fingerprint and the position share a slot, so a lookup reads one
place: 11 bytes per slot on the 2.4M-prefix benchmark fixture.

### BBHash

Level 0 is a bit vector of about 2 bits per prefix (gamma 2). Each key hash sets the bit chosen by a splitmix64 mix of the hash and the level number. Bits that two keys share are cleared, and those keys move on to the next level, which is sized for them alone. A key's slot is the number of set bits before its own, across all levels. On load the reader builds a rank directory with one count per 512 bits, so that count takes at most eight popcounts. A lookup therefore costs O(levels) however many prefixes the index holds. `mph.bin` holds about 3.3 bits per prefix; with the in-memory rank directory the table is about 3.7 bits per prefix.

### Lookup Algorithm

1. Hash the prefix with wyhash (final v4.3, `pkg/format/wyhash.go`) and the key seed
2. Find the key hash's BBHash slot; a hash that sets no bit on any level is not in the index
3. Read that slot from `mph_slots.bin`
4. Hash the prefix again with the fixed fingerprint seed; if it matches the slot's fingerprint, return the slot's position, otherwise the prefix doesn't exist

wyhash reads 8 bytes at a time; the byte-at-a-time FNV it replaced was
about 30% of a warm lookup. A non-member query is misreported only if
it lands on an occupied slot and its 64-bit fingerprint also matches
(~2⁻⁶⁴ per lookup). Checking the stored prefix as well would make hits
exact, but walking the prefix tree adds 51–90% LLC misses per lookup
(measured, #53), so lookups stay fingerprint-only.

### Key Seed

BBHash needs distinct keys. If two prefixes share a 64-bit key hash, the builder rehashes every prefix with a new seed (up to 8 tries). The seed is always stored, even when it is 0, as the 8-byte word after the slots in `mph_slots.bin`, and readers hash queries with it. The fingerprint uses its own fixed seed, which no retry can pick, so a retry never changes a fingerprint.

## Prefix Strings

Every non-root prefix is its parent prefix, one segment, then a slash:
`data/2024/` is `data/` + `2024` + `/`. The prefix tree stores exactly
that, so naming a child is one row read and rebuilding a prefix is one
row read per level.

| File | Description |
|------|-------------|
| `prefix_dict.bin` | Every distinct segment once, concatenated (no separators) |
| `prefix_dict.off.u64` | Byte offset for each segment in `prefix_dict.bin`; trailing sentinel marks the end |
| `prefix_tree.bin` | Per prefix in preorder: its last segment's ID and the distance back to its parent's position |

`prefix_tree.bin` layout:

| Offset | Size | Content |
|--------|------|---------|
| 0 | 20 | Header (`Count` = prefixes, `Width` = row bytes) |
| 20 | 2 | Descriptor: segment ID width (1–4), parent distance width (1–8) |
| 22 | `Count × Width` | Rows: segment ID, then parent distance |
| end | 8 | Zero tail pad |

Both widths come from the largest value in this index. The root `""`
is position 0 and its row is zero; every other row has a distance of at
least 1.

To read the prefix at position `p`, walk to the root collecting segment
IDs (`p -= distance` until `p == 0`), then join their segments in
reverse, each followed by `/`. The listing name of `p` (its last
segment) is the segment of its own row.

Because the builder writes each prefix as its parent plus one segment,
it requires the root first and every prefix after its parent;
`IndexBuilder.Add` rejects anything else with `ErrMissingParent`. The
aggregator always emits every ancestor.

On Open, all segments are preloaded into an in-memory cache (unique
segments are few next to prefixes: 9,262 segments for 2.37M prefixes
on the 1M-object benchmark fixture), so the warm-path cost is one
cache lookup per segment rather than a blob read.

## Tier Statistics

Tier stats give each prefix's `(count, bytes)` per tier listed in
`tiers.json`; tier slot *i* is the *i*-th tier there (ascending tier
ID). The pipeline declares the present tiers (those with data anywhere
in the inventory) before the first row, so tiers with no data get no
slot. A builder that declares none records all 13 tiers.

Every prefix's per-tier counts and bytes sum exactly to its
`object_count` and `total_bytes`; the builder checks this for every row
and fails the build otherwise. So the files store only what the core
row doesn't already give:

- a prefix whose `tier_mask` has one bit holds all its objects in that
  tier: its totals are the tier's stats;
- in a prefix with several, the highest tier in the mask is the totals
  minus the other tiers.

An index with one present tier writes no tier files at all. Otherwise
the builder computes the exact size of both layouts below at finalize
and writes the smaller (dense on a tie). Each stored slot is a count
then bytes, little-endian, at the widths the layout's largest stored
values need (1–8 bytes each), recorded in a 2-byte descriptor (count
width, bytes width) after the header of `tier_dense.bin` and
`tier_multi.bin`. Both files end with an 8-byte zero tail pad.

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

### Dense (`tier_dense.bin`)

Header (`Count` = prefixes, `Width` = (T − 1) × slot width for T
present tiers), the slot-width descriptor, then one row per prefix at
its preorder position holding slots 0 … T − 2, zeros included. Slot
T − 1 is derived. Readers check the row count and width against the
core stats and `tiers.json`.

### Multi (`tier_multi.bin`, `tier_multi.rows`, `tier_multi.off.u64`)

Only prefixes with two or more tiers store anything:

| File | Content |
|------|---------|
| `tier_multi.bin` | Header (`Count` = stored slots, `Width` = slot width), slot-width descriptor, then for each such prefix in preorder the slots of every tier in its mask except the highest, in slot order |
| `tier_multi.rows` | Header (`Count` = prefixes), a bit vector with bit *pos* set for each such prefix (little-endian `uint64` words), then a rank directory: for every 8 words, the set bits before them |
| `tier_multi.off.u64` | A variable-width array (see above): for the *k*-th such prefix, the index of its first slot in `tier_multi.bin`, plus a trailing sentinel |

To read prefix *pos*: `rank(pos)` (a directory entry plus at most eight
popcounts) gives *k*; its slots start at `off[k]`.

## Manifest

`manifest.json` lists every index file with its size and hex SHA-256
checksum, plus the prefix count and maximum depth. `verify` and
`format.VerifyManifest` check each file's size and checksum against it.

```json
{
  "created_at": "2026-01-02T03:04:05Z",
  "files": {
    "core_stats.bin": {"checksum": "9f2c…", "size": 37988049},
    "tier_stats/tier_multi.bin": {"checksum": "…", "size": 2567019},
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

The format version in file headers lets a reader refuse files it would mis-decode. Version 9 is the current format; readers accept only that version, and an index written by any other version is rebuilt.
