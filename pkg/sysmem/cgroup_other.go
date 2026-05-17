//go:build !linux

package sysmem

// cgroupMemoryMax has no implementation outside Linux. Returning
// (0, false) tells ApplyMemoryLimit there's no cgroup signal to mix
// in with the env / sysmem candidates.
func cgroupMemoryMax() (uint64, bool) { return 0, false }
