# Plan: Storage Tier Pricing Support

## Overview

Add support for tracking storage costs by S3 storage class and Intelligent-Tiering access tier. This enables cost analysis queries like "How much does the `data/2024/` prefix cost per month?" with breakdown by tier.

## S3 Storage Classes

**Standard Storage Classes** (mutually exclusive):
| Class | Inventory Value |
|-------|-----------------|
| Standard | `STANDARD` |
| Standard-IA | `STANDARD_IA` |
| One Zone-IA | `ONEZONE_IA` |
| Glacier Instant Retrieval | `GLACIER_IR` |
| Glacier Flexible Retrieval | `GLACIER` |
| Glacier Deep Archive | `DEEP_ARCHIVE` |
| Reduced Redundancy | `REDUCED_REDUNDANCY` |
| Intelligent-Tiering | `INTELLIGENT_TIERING` |

**Intelligent-Tiering Access Tiers** (for objects in IT):
| Tier | Inventory Value |
|------|-----------------|
| Frequent Access | `FREQUENT_ACCESS` |
| Infrequent Access | `INFREQUENT_ACCESS` |
| Archive Instant Access | `ARCHIVE_INSTANT_ACCESS` |
| Archive Access | `ARCHIVE_ACCESS` |
| Deep Archive Access | `DEEP_ARCHIVE_ACCESS` |

**Total categories to track: 12** (7 standard classes + 5 IT tiers, excluding IT class itself since we track its tiers)

## Storage Strategy Analysis

### Option A: Dense Columnar Arrays (Recommended)

Store one u64 array per tier, same as existing `total_bytes.u64`:

```
tier_stats/
├── standard.u64           # 8N bytes
├── standard_ia.u64        # 8N bytes
├── onezone_ia.u64         # 8N bytes
├── glacier_ir.u64         # 8N bytes
├── glacier_fr.u64         # 8N bytes
├── deep_archive.u64       # 8N bytes
├── reduced_redundancy.u64 # 8N bytes
├── it_frequent.u64        # 8N bytes
├── it_infrequent.u64      # 8N bytes
├── it_archive_instant.u64 # 8N bytes
├── it_archive.u64         # 8N bytes
└── it_deep_archive.u64    # 8N bytes
```

**Pros:**
- Consistent with existing columnar design
- O(1) access to any tier for any prefix
- mmap-friendly, cache-efficient
- Simple implementation

**Cons:**
- Fixed ~96 bytes per prefix overhead
- Most entries will be zero (sparse data)

**Space impact:**
- Current: ~80 bytes/prefix
- With tiers: ~176 bytes/prefix (+120%)
- 1M prefixes: 80MB → 176MB

### Option B: Sparse Storage

Store only non-zero tiers per node:
```
tier_data.bin:  [node0: bitmask, tier1_bytes, tier2_bytes, ...][node1: ...]
tier_offsets.u64: offset to each node's data
```

**Pros:**
- Minimal space for sparse data
- Could be 50-70% smaller if most prefixes use 1-2 tiers

**Cons:**
- Variable-size records complicate mmap
- Extra indirection for reads
- More complex aggregation logic

### Option C: Optimize Empty Tiers

Only create files for tiers that exist in the inventory:
```
tier_stats/
├── standard.u64           # Always present
├── glacier_fr.u64         # Only if Glacier objects exist
└── it_frequent.u64        # Only if IT objects exist
```

**Pros:**
- No overhead for unused tiers
- Still O(1) access

**Cons:**
- Slightly more complex manifest
- Still ~8 bytes per prefix per used tier

## Recommended Approach: Option A with C optimization

1. Use dense columnar arrays (Option A) for simplicity and query performance
2. Only create tier files that have non-zero data (Option C)
3. Store tier presence in manifest

## Implementation Plan

### Phase 1: Extend Inventory Reader

**File:** `pkg/inventory/reader.go`

```go
type Record struct {
    Key                      string
    Size                     uint64
    StorageClass             StorageClass  // NEW
    IntelligentTieringTier   ITTier        // NEW (optional)
}

type StorageClass uint8
const (
    StorageClassStandard StorageClass = iota
    StorageClassStandardIA
    StorageClassOneZoneIA
    StorageClassGlacierIR
    StorageClassGlacierFR
    StorageClassDeepArchive
    StorageClassReducedRedundancy
    StorageClassIntelligentTiering
    StorageClassUnknown
)

type ITTier uint8
const (
    ITTierNone ITTier = iota  // Not in IT
    ITTierFrequent
    ITTierInfrequent
    ITTierArchiveInstant
    ITTierArchive
    ITTierDeepArchive
)
```

**Changes:**
- Add column detection for `StorageClass` and `IntelligentTieringAccessTier`
- Parse string values to enum types
- Handle missing columns gracefully (default to Standard)

### Phase 2: Extend Trie Builder

**File:** `pkg/triebuild/builder.go`

```go
type Node struct {
    Prefix            string
    Pos               uint64
    Depth             uint32
    SubtreeEnd        uint64
    ObjectCount       uint64
    TotalBytes        uint64
    MaxDepthInSubtree uint32

    // NEW: Per-tier byte counts
    TierBytes         [12]uint64  // Indexed by TierIndex
}

type TierIndex uint8
const (
    TierStandard TierIndex = iota
    TierStandardIA
    TierOneZoneIA
    TierGlacierIR
    TierGlacierFR
    TierDeepArchive
    TierReducedRedundancy
    TierITFrequent
    TierITInfrequent
    TierITArchiveInstant
    TierITArchive
    TierITDeepArchive
    NumTiers
)
```

**Changes:**
- Track per-tier bytes in stack nodes
- Aggregate tier bytes when closing nodes (same as TotalBytes)
- Map StorageClass + ITTier to TierIndex

### Phase 3: Extend Index Format

**File:** `pkg/format/tiers.go` (new)

```go
type TierArrayWriter struct {
    writers [NumTiers]*ArrayWriter
    present [NumTiers]bool  // Track which tiers have data
}

func (w *TierArrayWriter) WriteTierBytes(tierBytes [NumTiers]uint64) error {
    for i, bytes := range tierBytes {
        if bytes > 0 {
            w.present[i] = true
        }
        if w.writers[i] != nil {
            w.writers[i].WriteU64(bytes)
        }
    }
    return nil
}
```

**Changes:**
- Add tier array writing to index builder
- Store tier presence in manifest
- Only write non-empty tier files

### Phase 4: Extend Index Reader

**File:** `pkg/indexread/index.go`

```go
type TierBreakdown struct {
    Standard          uint64
    StandardIA        uint64
    OneZoneIA         uint64
    GlacierIR         uint64
    GlacierFR         uint64
    DeepArchive       uint64
    ReducedRedundancy uint64
    ITFrequent        uint64
    ITInfrequent      uint64
    ITArchiveInstant  uint64
    ITArchive         uint64
    ITDeepArchive     uint64
}

func (idx *Index) TierBreakdown(pos uint64) TierBreakdown
func (idx *Index) TierBreakdownForPrefix(prefix string) (TierBreakdown, bool)
```

**Changes:**
- mmap tier arrays (only those present)
- Return zero for missing tier arrays
- Add TierBreakdown query methods

### Phase 5: Add Pricing Computation

**File:** `pkg/pricing/pricing.go` (new)

```go
// PriceTable holds per-GB-month prices in USD (microdollars for precision)
type PriceTable struct {
    Region            string
    Standard          uint64  // microdollars per GB-month
    StandardIA        uint64
    OneZoneIA         uint64
    GlacierIR         uint64
    GlacierFR         uint64
    DeepArchive       uint64
    ReducedRedundancy uint64
    ITFrequent        uint64
    ITInfrequent      uint64
    ITArchiveInstant  uint64
    ITArchive         uint64
    ITDeepArchive     uint64
}

type Cost struct {
    TotalMicrodollars uint64
    ByTier            [NumTiers]uint64
}

func ComputeMonthlyCost(breakdown TierBreakdown, prices PriceTable) Cost {
    // cost = (bytes / 1GB) * price_per_gb
    // Use fixed-point arithmetic to avoid float precision issues
}

// Default price tables for common regions
var USEast1Prices = PriceTable{
    Region:     "us-east-1",
    Standard:   23000,  // $0.023/GB = 23000 microdollars
    StandardIA: 12500,  // $0.0125/GB
    // ...
}
```

**Why compute at runtime (not store)?**
1. Prices change over time
2. Prices vary by region
3. User may want custom pricing
4. Storage cost is significant (8 bytes per prefix)

### Phase 6: Update CLI

```bash
# Build with tier tracking
s3inv-index build --track-tiers ...

# Query with cost breakdown
s3inv-index query --prefix "data/2024/" --cost --region us-east-1
```

## Data Flow

```
Inventory CSV
    │
    ▼ (parse StorageClass, ITTier)
┌─────────────────┐
│ Inventory Reader │
└────────┬────────┘
         │ Record{Key, Size, StorageClass, ITTier}
         ▼
┌─────────────────┐
│  External Sort  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Trie Builder   │ ── accumulate TierBytes[12] per node
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Format Writer  │ ── write tier_stats/*.u64 files
└────────┬────────┘
         │
         ▼
    Index Files
         │
         ▼
┌─────────────────┐
│  Index Reader   │ ── mmap tier arrays
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Pricing Engine  │ ── compute cost from TierBreakdown + PriceTable
└─────────────────┘
```

## Backward Compatibility

- Tier tracking is **opt-in** via build flag
- Indexes without tier data continue to work
- `TierBreakdown()` returns zeros for non-tier indexes
- Manifest includes `has_tier_data: true/false`

## Testing Plan

1. Unit tests for storage class parsing
2. Unit tests for tier aggregation in trie builder
3. Integration tests for tier data round-trip
4. Benchmark tier queries vs non-tier queries
5. Test with real S3 inventory samples

## Open Questions

1. **Should we store object counts per tier?**
   - Adds 12 more u64 arrays (~96 more bytes/prefix)
   - Useful for "how many Glacier objects?" queries
   - Recommendation: Yes, add `tier_object_count/` arrays

2. **Should we support custom tier mappings?**
   - Some users may have Outposts or custom classes
   - Recommendation: Add "unknown" tier, log warnings

3. **Should we precompute costs during build?**
   - Saves runtime computation
   - Requires specifying region at build time
   - Recommendation: No, compute at runtime for flexibility

## Timeline

| Phase | Description | Estimated Effort |
|-------|-------------|------------------|
| 1 | Extend inventory reader | Small |
| 2 | Extend trie builder | Small |
| 3 | Extend index format | Medium |
| 4 | Extend index reader | Small |
| 5 | Add pricing computation | Small |
| 6 | Update CLI | Small |
| - | Testing & documentation | Medium |
