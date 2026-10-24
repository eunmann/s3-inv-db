package sysmem

import "testing"

func TestResolveLimit(t *testing.T) {
	const gib = int64(1) << 30
	cases := []struct {
		name       string
		r          MemoryLimitResult
		explicit   bool
		wantBytes  int64
		wantSource MemoryLimitSource
	}{
		{"auto keeps headroom below the cgroup", MemoryLimitResult{CgroupBytes: 10 * gib, SysmemFractionBytes: 100 * gib}, false, int64(float64(10*gib) * CgroupHeadroomFraction), MemoryLimitSourceCgroup},
		{"auto takes the smaller sysmem fraction", MemoryLimitResult{CgroupBytes: 10 * gib, SysmemFractionBytes: 4 * gib}, false, 4 * gib, MemoryLimitSourceSysmem},
		{"explicit env wins below the cgroup", MemoryLimitResult{EnvBytes: 9 * gib, CgroupBytes: 10 * gib}, true, 9 * gib, MemoryLimitSourceEnv},
		{"explicit env is capped by the raw cgroup", MemoryLimitResult{EnvBytes: 20 * gib, CgroupBytes: 10 * gib}, true, 10 * gib, MemoryLimitSourceCgroup},
		{"nothing detected", MemoryLimitResult{}, false, 0, MemoryLimitSourceDefault},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bytes, source := resolveLimit(c.r, c.explicit)
			if bytes != c.wantBytes || source != c.wantSource {
				t.Errorf("resolveLimit = %d (%s), want %d (%s)", bytes, source, c.wantBytes, c.wantSource)
			}
		})
	}
}
