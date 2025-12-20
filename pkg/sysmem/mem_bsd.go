//go:build freebsd || openbsd || netbsd || dragonfly

package sysmem

import "golang.org/x/sys/unix"

// totalSystemMemory returns total system RAM on BSD variants using sysctl.
func totalSystemMemory() (uint64, bool) {
	// hw.physmem returns total physical memory in bytes on BSD systems
	// Try hw.physmem first (available on most BSDs)
	mem, err := unix.SysctlUint64("hw.physmem")
	if err == nil && mem > 0 {
		return mem, true
	}

	// Fallback: try hw.realmem on FreeBSD
	mem, err = unix.SysctlUint64("hw.realmem")
	if err == nil && mem > 0 {
		return mem, true
	}

	return 0, false
}
