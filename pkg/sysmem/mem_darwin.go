//go:build darwin

package sysmem

import "golang.org/x/sys/unix"

// totalSystemMemory returns total system RAM on macOS using sysctl.
func totalSystemMemory() (uint64, bool) {
	// hw.memsize returns total physical memory in bytes
	mem, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0, false
	}
	return mem, true
}
