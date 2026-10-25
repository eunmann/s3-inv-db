//go:build !linux

package benchutil

import "testing"

// DropPageCache needs posix_fadvise, which only Linux provides here:
// cold-cache benchmarks skip elsewhere.
func DropPageCache(tb testing.TB, _ string) {
	tb.Helper()
	tb.Skip("page-cache eviction needs Linux")
}
