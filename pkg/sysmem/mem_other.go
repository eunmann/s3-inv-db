//go:build !linux && !darwin && !windows && !freebsd && !openbsd && !netbsd && !dragonfly

package sysmem

// totalSystemMemory returns a fallback for unsupported platforms.
// Returns false to indicate the value is not reliable.
func totalSystemMemory() (uint64, bool) {
	// On unsupported platforms, return 0 to trigger the default fallback
	return 0, false
}
