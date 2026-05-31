package extsort

import (
	"fmt"
	"testing"
)

// BenchmarkMergeHeap measures one push+pop cycle on the merge heap
// across realistic heap sizes (k=8/32/64). Rows are pre-allocated in
// a ring outside the timed region so the measurement reflects heap
// movement, not Sprintf+allocation.
func BenchmarkMergeHeap(b *testing.B) {
	for _, k := range []int{8, 32, 64} {
		b.Run(fmt.Sprintf("k=%d", k), func(b *testing.B) {
			h := &typedMergeHeap{items: make([]mergeItem, 0, k)}
			for i := range k {
				row := &PrefixRow{Prefix: fmt.Sprintf("tenant-%05d/year=2024/month=%02d/file.parquet", i*7919, (i%12)+1)}
				h.push(mergeItem{row: row, readerIdx: i})
			}

			const ringSize = 1024
			ring := make([]*PrefixRow, ringSize)
			for i := range ring {
				ring[i] = &PrefixRow{Prefix: fmt.Sprintf("zzzz-%08d", i)}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				top := h.pop()
				h.push(mergeItem{row: ring[i%ringSize], readerIdx: top.readerIdx})
			}
		})
	}
}
