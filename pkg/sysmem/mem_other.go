//go:build !linux && !darwin && !windows && !freebsd && !openbsd && !netbsd && !dragonfly

package sysmem

// totalSystemMemory has no implementation on unsupported platforms.
func totalSystemMemory() (uint64, bool) { return 0, false }

// cgroupMemoryMax is Linux-only.
func cgroupMemoryMax() (uint64, bool) { return 0, false }
