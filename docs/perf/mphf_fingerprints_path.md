# MPHF Fingerprint Pipeline - Data Flow Documentation

This document maps the fingerprint computation path during MPHF build, focusing on code in this repository (not BBHash internals).

## Overview

The fingerprint pipeline transforms S3 object prefixes into a minimal perfect hash function (MPHF) with fingerprints for collision detection. The key phases are:

```
Prefix String → Hash Key → BBHash Construction → Fingerprint Computation → Disk Storage
```

## Data Structures

### PrefixRow (`pkg/extsort/types.go:38`)
Input data containing prefix strings with statistics.

### StreamingMPHFBuilder (`pkg/format/mphf_streaming.go:28-41`)
```go
type StreamingMPHFBuilder struct {
    hashes      []uint64      // Hash keys for BBHash (8 bytes/prefix)
    preorderPos []uint64      // Original preorder positions (8 bytes/prefix)
    tempFile    *os.File      // Temp file for prefix strings
    tempWriter  *bufio.Writer // 1MB buffered writer
    count       uint64        // Number of prefixes added
    bufferSize  int           // 1MB buffer for I/O
}
```

**Memory footprint**: 16 bytes per prefix in-memory; prefix strings go to disk.

---

## Phase 1: Add (Streaming)

**File**: `pkg/format/mphf_streaming.go`
**Function**: `Add(prefix string, pos uint64)` (lines 65-88)

```
For each prefix:
1. hashString(prefix) → uint64         [FNV-1a hash]
2. Append hash to b.hashes slice
3. Append pos to b.preorderPos slice
4. Write [4-byte length][prefix bytes] to temp file
```

### Hash Function for Keys (`pkg/format/mphf.go:340-350`)
```go
func hashString(s string) uint64 {
    h := fnv.New64a()      // FNV-1a 64-bit
    h.Write([]byte(s))
    return h.Sum64()
}
```

**Allocation**: `[]byte(s)` creates a copy of the string.

---

## Phase 2: BBHash Construction

**File**: `pkg/format/mphf_streaming.go`
**Location**: `Build()` method, lines 122-130

```go
mph, err := bbhash.New(b.hashes, bbhash.Gamma(2.0))
```

- Input: `[]uint64` of hash keys
- Output: `*bbhash.BBHash2` (minimal perfect hash function)
- Maps arbitrary uint64 → 1..N positions

After construction, hash keys are freed:
```go
b.hashes = nil
runtime.GC()
```

---

## Phase 3: Fingerprint Computation (Parallel)

**File**: `pkg/format/mphf_streaming.go`
**Functions**:
- `computeFingerprintsParallel()` (lines 224-276)
- `processChunk()` (lines 278-361)

### Architecture
```
Workers: runtime.NumCPU() goroutines
Chunk Size: 50,000 prefixes per batch
Buffer Strategy: Pre-allocated contiguous buffer per chunk
```

### Per-Prefix Processing (lines 250-275)
```go
for _, item := range work.items {
    // 1. Hash for MPHF lookup
    keyHash := hashBytes(item.prefixBytes)

    // 2. Get MPHF position
    hashVal := mph.Find(keyHash)
    hashPos := int(hashVal - 1)

    // 3. Compute fingerprint (different hash!)
    fingerprints[hashPos] = computeFingerprintBytes(item.prefixBytes)

    // 4. Store preorder position
    preorderPositions[hashPos] = b.preorderPos[item.index]

    // 5. Store blob offset
    orderedPrefixOffsets[hashPos] = item.offset
}
```

### Fingerprint Hash Function (`pkg/format/mphf.go:355-369`)
```go
func computeFingerprintBytes(b []byte) uint64 {
    h := fnv.New64()       // FNV-1 (NOT FNV-1a!)
    h.Write(b)
    return h.Sum64()
}
```

**Important**: Uses FNV-1 (not FNV-1a) for fingerprints to ensure independence from key hash.

### Chunk Buffer Optimization (lines 293-334)
```go
// Pre-allocate contiguous buffer for chunk
chunkBuffer := make([]byte, 0, thisChunk*estimatedAvgPrefixLen)

for range thisChunk {
    start := len(chunkBuffer)
    chunkBuffer = chunkBuffer[:start+int(prefixLen)]
    io.ReadFull(reader, chunkBuffer[start:])

    items = append(items, prefixChunkItem{
        prefixBytes: chunkBuffer[start : start+int(prefixLen)],
        // ...
    })
}
```

Slices into buffer avoid per-prefix allocations during fingerprint computation.

---

## Phase 4: Disk Storage

### Output Files

| File | Format | Purpose |
|------|--------|---------|
| `mph.bin` | Binary | Serialized BBHash structure |
| `mph_fp.u64` | Array | Fingerprints for verification |
| `mph_pos.u64` | Array | MPHF position → preorder position |
| `prefix_blob.bin` | Raw | Concatenated prefix strings |
| `prefix_offsets.u64` | Array | Offset into blob per prefix |

### ArrayWriter (`pkg/format/writer.go`)
Writes uint64 arrays with buffered I/O:
```
Header: [magic 4B][version 4B][count 8B][width 4B]
Data:   [N × 8-byte little-endian uint64 values]
```

---

## Lookup Path (Query Time)

**File**: `pkg/format/mphf.go`
**Function**: `Lookup(prefix string)` (lines 275-302)

```go
func (m *MPHF) Lookup(prefix string) (pos uint64, ok bool) {
    keyHash := hashString(prefix)
    hashVal := m.mph.Find(keyHash)

    if hashVal == 0 {
        return 0, false
    }

    hashPos := hashVal - 1

    // Fingerprint verification
    storedFP := m.fingerprints.UnsafeGetU64(hashPos)
    computedFP := computeFingerprint(prefix)

    if storedFP != computedFP {
        return 0, false  // False positive detected
    }

    return m.preorderPos.UnsafeGetU64(hashPos), true
}
```

---

## Hot Path Summary

### Build-Time Hot Path
1. **hashString()** - called once per prefix during Add
2. **hashBytes()** - called once per prefix during fingerprint computation
3. **mph.Find()** - called once per prefix to get MPHF position
4. **computeFingerprintBytes()** - called once per prefix

### Potential Bottlenecks (Investigate)
1. `fnv.New64a()` / `fnv.New64()` - allocates hasher state each call
2. `[]byte(s)` conversion - string to bytes copy
3. Per-prefix hash function overhead
4. BBHash internal operations during Find()

---

## Code Locations Quick Reference

| Operation | File | Lines |
|-----------|------|-------|
| StreamingMPHFBuilder struct | `mphf_streaming.go` | 28-41 |
| Add method | `mphf_streaming.go` | 65-88 |
| Build method | `mphf_streaming.go` | 109-211 |
| Fingerprint parallel compute | `mphf_streaming.go` | 224-276 |
| Chunk processing | `mphf_streaming.go` | 278-361 |
| hashString / hashBytes | `mphf.go` | 340-354 |
| computeFingerprint | `mphf.go` | 355-369 |
| Lookup verification | `mphf.go` | 275-302 |
| ArrayWriter | `writer.go` | 50-140 |
