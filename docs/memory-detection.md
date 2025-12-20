# Memory Detection

This document describes how s3-inv-db detects system memory and configures memory budgets.

## Overview

The `pkg/sysmem` package provides cross-platform system memory detection. This is used to automatically configure the aggregator memory threshold for the external sort pipeline.

## Supported Platforms

| Platform | Method | Notes |
|----------|--------|-------|
| Linux | `unix.Sysinfo` | Uses `Totalram * Unit` |
| macOS (Darwin) | `sysctl hw.memsize` | Returns total physical memory |
| Windows | `GlobalMemoryStatusEx` | Uses `TotalPhys` field |
| FreeBSD, OpenBSD, NetBSD, DragonFlyBSD | `sysctl hw.physmem` | Falls back to `hw.realmem` on FreeBSD |
| Other platforms | Fallback | Returns 4 GB default |

## API

```go
import "github.com/eunmann/s3-inv-db/pkg/sysmem"

// Get total system memory with reliability indicator
result := sysmem.Total()
fmt.Printf("Total: %d bytes, Reliable: %v\n", result.TotalBytes, result.Reliable)

// Convenience function for just the value
bytes := sysmem.TotalBytes()
```

### Result Structure

```go
type Result struct {
    TotalBytes uint64  // Total system memory in bytes
    Reliable   bool    // true if platform-specific detection succeeded
}
```

## Fallback Behavior

When platform-specific detection fails or is unsupported:

- `TotalBytes` returns `DefaultMemoryBytes` (4 GB)
- `Reliable` is set to `false`

This conservative default ensures the build process doesn't consume excessive memory on unknown platforms.

## Memory Threshold Configuration

The aggregator memory threshold is set in `DefaultConfig()`:

```go
// 25% of total RAM, clamped to [128MB, 1GB]
memResult := sysmem.Total()
memThreshold := int64(memResult.TotalBytes) / 4

// Clamp to reasonable bounds
if memThreshold < 128*1024*1024 {
    memThreshold = 128 * 1024 * 1024  // Min 128MB
} else if memThreshold > 1024*1024*1024 {
    memThreshold = 1024 * 1024 * 1024 // Max 1GB
}
```

### Example Thresholds

| System RAM | Calculated Threshold |
|------------|---------------------|
| 2 GB | 512 MB → clamped to 512 MB |
| 4 GB | 1 GB → clamped to 1 GB |
| 8 GB | 2 GB → clamped to 1 GB |
| 16 GB | 4 GB → clamped to 1 GB |
| 32 GB | 8 GB → clamped to 1 GB |

## CLI Override

The memory threshold can be overridden via CLI flags (if implemented):

```bash
s3inv-index build --memory-threshold 512MB ...
```

Or via environment variable:

```bash
S3INV_MEMORY_THRESHOLD=512MB s3inv-index build ...
```

## Build Tags

Platform-specific implementations use Go build constraints:

- `mem_linux.go`: `//go:build linux`
- `mem_darwin.go`: `//go:build darwin`
- `mem_windows.go`: `//go:build windows`
- `mem_bsd.go`: `//go:build freebsd || openbsd || netbsd || dragonfly`
- `mem_other.go`: `//go:build !linux && !darwin && !windows && !freebsd && !openbsd && !netbsd && !dragonfly`

This ensures the code compiles on all platforms without CGO dependencies.

## Testing

Run tests with:

```bash
go test ./pkg/sysmem/... -v
```

The tests verify:
- Memory is detected (non-zero)
- Values are reasonable (≥ 1 GB, ≤ 1 TB)
- Fallback behavior works on unsupported platforms
- `Reliable` flag matches expectations for the current platform
