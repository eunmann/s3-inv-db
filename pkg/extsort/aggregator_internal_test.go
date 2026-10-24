package extsort

import (
	"runtime"
	"testing"
)

// heapInuseBytes must report what runtime.ReadMemStats calls HeapInuse.
func TestHeapInuseBytesMatchesMemStats(t *testing.T) {
	keep := make([][]byte, 0, 64)
	for range 64 {
		keep = append(keep, make([]byte, 1<<16))
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	got := heapInuseBytes()
	runtime.KeepAlive(keep)

	lo, hi := m.HeapInuse*9/10, m.HeapInuse*11/10
	if got < lo || got > hi {
		t.Errorf("heapInuseBytes = %d, MemStats.HeapInuse = %d (want within 10%%)", got, m.HeapInuse)
	}
}
