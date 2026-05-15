package extsort

import (
	"fmt"
	"testing"
)

// BenchmarkMergeHeap measures one push+pop cycle on the merge heap.
// The current implementation (typed, hand-rolled in merger.go +
// parallel_merge.go) is interface{}-free and should report 0 allocs/op.
func BenchmarkMergeHeap(b *testing.B) {
	for _, k := range []int{8, 32, 64} {
		b.Run(fmt.Sprintf("k=%d", k), func(b *testing.B) {
			h := &mergeHeap{items: make([]mergeItem, 0, k)}
			for i := range k {
				row := &PrefixRow{Prefix: fmt.Sprintf("tenant-%05d/year=2024/month=%02d/file.parquet", i*7919, (i%12)+1)}
				h.push(mergeItem{row: row, readerIdx: i})
			}

			next := 0
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				top := h.pop()
				next++
				row := &PrefixRow{Prefix: fmt.Sprintf("zzzz-%08d", next)}
				h.push(mergeItem{row: row, readerIdx: top.readerIdx})
			}
		})
	}
}
