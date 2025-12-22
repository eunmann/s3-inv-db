# Stage 1: Manifest Discovery (`stage_manifest`)

## Stage Summary

**What this stage does:** Fetches the S3 inventory manifest.json file from S3, parses its JSON content, and extracts metadata including the list of inventory chunk files, schema information, and format detection.

**Inputs:**
- S3 URI to manifest.json (e.g., `s3://bucket/inventory/2024-01-01/manifest.json`)
- Typical size: 1-50 KB JSON file

**Outputs:**
- Parsed `Manifest` struct containing:
  - `Files[]`: list of inventory chunk files with keys and sizes
  - `FileSchema`: CSV column names (e.g., `"Bucket, Key, Size, StorageClass"`)
  - `FileFormat`: "CSV" or "Parquet"
  - `DestinationBucket`: S3 bucket containing inventory files

---

## Control Flow and Data Flow

### Main Functions

```
cli.runBuild()
    └── s3fetch.Client.FetchManifest(bucket, key)
            ├── s3.GetObject() [S3 API call]
            └── ParseManifest(resp.Body)
                    ├── json.NewDecoder().Decode()
                    └── manifest.validate()
```

### Data Flow

1. **Input:** S3 URI string parsed via `ParseS3URI()`
2. **S3 Fetch:** Single `GetObject` API call downloads manifest JSON
3. **Parse:** Streaming JSON decode into `Manifest` struct
4. **Validation:** Check required fields present
5. **Output:** `*Manifest` returned to caller

### Key Files and Line References

| File | Function | Lines | Purpose |
|------|----------|-------|---------|
| `s3fetch/client.go` | `FetchManifest()` | 52-68 | Orchestrates fetch + parse |
| `s3fetch/manifest.go` | `ParseManifest()` | 40-51 | JSON decode + validate |
| `s3fetch/manifest.go` | `validate()` | 54-70 | Field validation |
| `s3fetch/manifest.go` | `DetectFormat()` | 76-98 | CSV vs Parquet detection |
| `s3fetch/manifest.go` | `columnIndex()` | 147-156 | Schema parsing |

---

## Performance Analysis

### CPU Hotspots

**Current:** None significant. JSON decode of 1-50KB is negligible.

**Complexity:**
- `ParseManifest()`: O(n) where n = JSON size
- `DetectFormat()`: O(1) - checks first file only
- `columnIndex()`: O(m) where m = number of columns (typically 5-10)

### Observations

1. **Streaming Decode:** Uses `json.NewDecoder()` which streams, avoiding loading entire body into memory.

2. **Duplicate Format Detection:** `DetectFormat()` is called multiple times in downstream code:
   - `cli.runBuild()` calls `manifest.IsParquet()` / `manifest.IsCSV()`
   - Each call re-parses the format

3. **Schema Parsing Inefficiency:** `columnIndex()` is called separately for Key, Size, StorageClass, AccessTier. Each call re-splits the schema string:
   ```go
   cols := strings.Split(m.FileSchema, ",")  // Called 4 times
   ```

---

## Memory Analysis

### Allocations

| Allocation | Size | Lifetime |
|------------|------|----------|
| `Manifest` struct | ~200 bytes + file list | Pipeline duration |
| `[]ManifestFile` | ~80 bytes per file | Pipeline duration |
| JSON decode buffers | ~4KB | Temporary |
| Schema split slice | ~10 strings | Per `columnIndex()` call |

### Peak Memory

Negligible. Manifest files are typically 1-50KB. With 1000 chunk files, the `[]ManifestFile` slice is ~80KB.

---

## Concurrency & Parallelism

### Current State

- **No concurrency** in this stage
- Single S3 `GetObject` call, single-threaded JSON parse
- This is appropriate given the small file size

### Potential Issues

None. The manifest is a single small file; parallelism would add complexity without benefit.

---

## I/O Patterns

### S3 Access

- **Pattern:** Single `GetObject` API call
- **Size:** 1-50 KB
- **Latency:** Dominated by S3 API round-trip (~50-200ms)

### Observations

1. **No retries configured:** The `GetObject` call doesn't have explicit retry logic (relies on SDK defaults).

2. **No caching:** Manifest is re-fetched on each run. For repeated runs on the same inventory, caching would save an API call.

---

## Concrete Recommendations

### Rec-1: Cache Column Indices

**Location:** `s3fetch/manifest.go`, `columnIndex()` function

**Current behavior:** Each call to `KeyColumnIndex()`, `SizeColumnIndex()`, etc. re-splits the schema string and iterates to find the column.

**Proposed change:** Parse all column indices once after manifest load:

```go
type Manifest struct {
    // ... existing fields ...

    // Cached column indices (computed once)
    keyIdx         int
    sizeIdx        int
    storageClassIdx int
    accessTierIdx  int
    indicesParsed  bool
}

func (m *Manifest) parseColumnIndices() {
    if m.indicesParsed {
        return
    }
    cols := strings.Split(m.FileSchema, ",")
    for i, col := range cols {
        col = strings.TrimSpace(col)
        switch strings.ToLower(col) {
        case "key":
            m.keyIdx = i
        case "size":
            m.sizeIdx = i
        case "storageclass":
            m.storageClassIdx = i
        case "intelligenttiering accesstier":
            m.accessTierIdx = i
        }
    }
    m.indicesParsed = true
}
```

**Expected benefit:**
- CPU: Eliminate ~4 redundant string splits and iterations
- Memory: Avoid ~4 temporary slice allocations per parse

**Risk level:** Low

**How to test:** Unit test `Manifest` methods; verify column indices match expected values.

---

### Rec-2: Cache Format Detection

**Location:** `s3fetch/manifest.go`, `DetectFormat()` function

**Current behavior:** `DetectFormat()` re-evaluates format on each call by checking `FileFormat` field and file extensions.

**Proposed change:** Cache the detected format after first computation:

```go
type Manifest struct {
    // ... existing fields ...
    detectedFormat InventoryFormat
    formatDetected bool
}

func (m *Manifest) DetectFormat() InventoryFormat {
    if m.formatDetected {
        return m.detectedFormat
    }
    // ... existing detection logic ...
    m.detectedFormat = format
    m.formatDetected = true
    return format
}
```

**Expected benefit:** Minimal (microseconds), but cleaner semantics

**Risk level:** Low

**How to test:** Existing unit tests should pass unchanged.

---

### Rec-3: Add Manifest Caching for Repeated Runs

**Location:** `internal/cli/cli.go` or new caching layer

**Current behavior:** Manifest is fetched from S3 on every run.

**Proposed change:** Optional local cache of manifest.json with TTL or etag validation:

```go
type ManifestCache struct {
    cacheDir string
    ttl      time.Duration
}

func (c *ManifestCache) FetchManifest(ctx context.Context, s3Client *Client, bucket, key string) (*Manifest, error) {
    cacheFile := filepath.Join(c.cacheDir, hash(bucket, key)+".json")
    if info, err := os.Stat(cacheFile); err == nil {
        if time.Since(info.ModTime()) < c.ttl {
            return loadCachedManifest(cacheFile)
        }
    }
    manifest, err := s3Client.FetchManifest(ctx, bucket, key)
    if err != nil {
        return nil, err
    }
    saveCachedManifest(cacheFile, manifest)
    return manifest, nil
}
```

**Expected benefit:** Save S3 API call (50-200ms) for repeated runs on same inventory

**Risk level:** Medium (cache invalidation complexity)

**How to test:** Integration test with mocked S3; verify cache hit/miss behavior.

---

## Summary

Stage 1 is simple and efficient. The manifest is a small JSON file, and the current implementation uses streaming decode which is appropriate.

**Priority of recommendations:**

| ID | Title | Priority | Impact |
|----|-------|----------|--------|
| Rec-1 | Cache column indices | Low | Negligible CPU savings |
| Rec-2 | Cache format detection | Low | Code cleanliness |
| Rec-3 | Manifest caching | Low | 50-200ms saved per run |

**Overall assessment:** No significant performance issues. This stage is not a bottleneck.
