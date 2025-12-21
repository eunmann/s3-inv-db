//go:build linux

package sysmem

import "golang.org/x/sys/unix"

// totalSystemMemory returns total system RAM on Linux using sysinfo.
func totalSystemMemory() (uint64, bool) {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0, false
	}
	// Total RAM in bytes = Totalram * Unit
	return info.Totalram * uint64(info.Unit), true
}
