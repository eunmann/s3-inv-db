package sysmem_test

import (
	"runtime"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/sysmem"
)

func TestTotal(t *testing.T) {
	result := sysmem.Total()

	// Should always return a positive value
	if result.TotalBytes == 0 {
		t.Error("Total() returned 0 bytes")
	}

	// Should be at least 1GB (reasonable minimum for any modern system)
	minExpected := uint64(1 * 1024 * 1024 * 1024)
	if result.TotalBytes < minExpected {
		t.Errorf("Total() returned %d bytes, expected at least %d", result.TotalBytes, minExpected)
	}

	// On supported platforms, should be reliable
	switch runtime.GOOS {
	case "linux", "darwin", "windows", "freebsd", "openbsd", "netbsd", "dragonfly":
		if !result.Reliable {
			t.Logf("Warning: memory detection not reliable on %s (may indicate permission issue)", runtime.GOOS)
		}
	default:
		// On other platforms, should use fallback
		if result.Reliable {
			t.Errorf("Expected Reliable=false on %s, got true", runtime.GOOS)
		}
		if result.TotalBytes != sysmem.DefaultMemoryBytes {
			t.Errorf("Expected fallback value %d on %s, got %d", sysmem.DefaultMemoryBytes, runtime.GOOS, result.TotalBytes)
		}
	}

	t.Logf("Detected memory: %d bytes (%.2f GB), reliable=%v",
		result.TotalBytes,
		float64(result.TotalBytes)/(1024*1024*1024),
		result.Reliable)
}

func TestDefaultMemoryBytes(t *testing.T) {
	// Default should be 4GB
	expected := uint64(4 * 1024 * 1024 * 1024)
	if sysmem.DefaultMemoryBytes != expected {
		t.Errorf("sysmem.DefaultMemoryBytes = %d, expected %d", sysmem.DefaultMemoryBytes, expected)
	}
}

func TestResultValues(t *testing.T) {
	result := sysmem.Total()

	// Should not exceed any reasonable physical limit (1TB)
	maxReasonable := uint64(1024 * 1024 * 1024 * 1024)
	if result.TotalBytes > maxReasonable {
		t.Errorf("Total() returned unreasonably large value: %d bytes", result.TotalBytes)
	}
}
