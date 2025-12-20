// Package sysmem provides cross-platform system memory detection.
//
// This package detects total system RAM using platform-specific methods
// and provides safe defaults for unsupported platforms.
package sysmem

// DefaultMemoryBytes is the fallback memory value (4 GB) used when
// platform-specific detection fails or is unsupported.
const DefaultMemoryBytes uint64 = 4 * 1024 * 1024 * 1024

// Result holds the result of memory detection.
type Result struct {
	// TotalBytes is the total system memory in bytes.
	TotalBytes uint64

	// Reliable indicates whether the value was obtained from
	// a platform-specific method (true) or is a fallback default (false).
	Reliable bool
}

// Total returns the total system memory.
// If platform-specific detection fails or is unsupported,
// it returns DefaultMemoryBytes with Reliable=false.
func Total() Result {
	bytes, ok := totalSystemMemory()
	if !ok || bytes == 0 {
		return Result{
			TotalBytes: DefaultMemoryBytes,
			Reliable:   false,
		}
	}
	return Result{
		TotalBytes: bytes,
		Reliable:   true,
	}
}

// TotalBytes is a convenience function that returns just the memory value.
// Use Total() if you need to know whether the value is reliable.
func TotalBytes() uint64 {
	return Total().TotalBytes
}
