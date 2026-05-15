package extsort

import (
	"container/heap"
	"fmt"
	"testing"
)

// BenchmarkMergeHeap measures the cost of one push+pop cycle on the
// container/heap-based merge heap. Surfaces interface{} boxing and
// string comparison overhead during the k-way merge.
func BenchmarkMergeHeap(b *testing.B) {
	for _, k := range []int{8, 32, 64} {
		b.Run(fmt.Sprintf("k=%d", k), func(b *testing.B) {
			h := &mergeHeap{items: make([]mergeItem, 0, k)}
			for i := range k {
				row := &PrefixRow{Prefix: fmt.Sprintf("tenant-%05d/year=2024/month=%02d/file.parquet", i*7919, (i%12)+1)}
				heap.Push(h, mergeItem{row: row, readerIdx: i})
			}
			heap.Init(h)

			// Steady-state: pop then push back a similar row so heap size stays at k.
			next := 0
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				topAny := heap.Pop(h)
				top := topAny.(mergeItem)
				next++
				row := &PrefixRow{Prefix: fmt.Sprintf("zzzz-%08d", next)}
				heap.Push(h, mergeItem{row: row, readerIdx: top.readerIdx})
			}
		})
	}
}
