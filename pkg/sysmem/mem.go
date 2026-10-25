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

// Available returns the memory the process can use: total system
// memory, capped by the cgroup v2 memory.max when one is set. In a
// container that is the container's limit, not the host's RAM.
func Available() Result {
	limit, ok := cgroupMemoryMax()

	return capByCgroup(Total(), limit, ok)
}

// capByCgroup lowers total to a set cgroup limit below it.
func capByCgroup(total Result, limit uint64, ok bool) Result {
	if !ok || limit >= total.TotalBytes {
		return total
	}

	return Result{TotalBytes: limit, Reliable: true}
}
