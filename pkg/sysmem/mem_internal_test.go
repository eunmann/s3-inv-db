package sysmem

import "testing"

func TestCapByCgroup(t *testing.T) {
	host := Result{TotalBytes: 128 << 30, Reliable: true}
	fallback := Result{TotalBytes: DefaultMemoryBytes}
	tests := []struct {
		name  string
		total Result
		limit uint64
		ok    bool
		want  Result
	}{
		{"no cgroup limit", host, 0, false, host},
		{"cgroup below host", host, 4 << 30, true, Result{TotalBytes: 4 << 30, Reliable: true}},
		{"cgroup above host", host, 256 << 30, true, host},
		{"cgroup below fallback", fallback, 1 << 30, true, Result{TotalBytes: 1 << 30, Reliable: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := capByCgroup(tt.total, tt.limit, tt.ok); got != tt.want {
				t.Errorf("capByCgroup(%+v, %d, %v) = %+v, want %+v", tt.total, tt.limit, tt.ok, got, tt.want)
			}
		})
	}
}
