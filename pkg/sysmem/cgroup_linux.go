//go:build linux

package sysmem

import (
	"os"
	"strconv"
	"strings"
)

// cgroupMemoryMax reads the cgroup v2 memory cap exposed at
// /sys/fs/cgroup/memory.max. Returns (limit, true) when a numeric cap
// is set; returns (0, false) for "max" (unlimited), missing files, or
// any read/parse error so callers fall back to other limit signals.
//
// Cgroup v1 hierarchies (the older /sys/fs/cgroup/memory/...) Are
// intentionally not probed: cgroup v2 has been the default on every
// distribution that ships modern containers for years, and v1 layouts
// vary enough that the false-positive risk outweighs the coverage.
func cgroupMemoryMax() (uint64, bool) {
	const path = "/sys/fs/cgroup/memory.max"
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	if s == "" || s == "max" {
		return 0, false
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n == 0 {
		return 0, false
	}

	return n, true
}
