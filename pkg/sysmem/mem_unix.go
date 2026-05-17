//go:build darwin || freebsd || openbsd || netbsd || dragonfly

package sysmem

import "golang.org/x/sys/unix"

// totalSystemMemory returns total system RAM on darwin/BSD via sysctl.
// Tries hw.memsize (darwin), then hw.physmem and hw.realmem (BSDs).
func totalSystemMemory() (uint64, bool) {
	for _, key := range []string{"hw.memsize", "hw.physmem", "hw.realmem"} {
		if mem, err := unix.SysctlUint64(key); err == nil && mem > 0 {
			return mem, true
		}
	}

	return 0, false
}

// cgroupMemoryMax has no meaning outside Linux; returning (0,false)
// tells ApplyMemoryLimit to skip the cgroup signal.
func cgroupMemoryMax() (uint64, bool) { return 0, false }
