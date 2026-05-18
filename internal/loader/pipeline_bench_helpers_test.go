package loader_test

import (
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// silenceZerologPipelineBench mutes the pipeline's internal logging
// so benchstat-style parsers see a clean output line.
func silenceZerologPipelineBench(b *testing.B) {
	b.Helper()
	prev := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(prev) })
}

// heapPeakSampler runs a goroutine that polls runtime.MemStats every
// 5ms and tracks the highest HeapAlloc seen. Stop() ends the sampler
// and returns the peak. Cheap enough that 5ms cadence doesn't move
// the timing needle on the run we're measuring.
type heapPeakSampler struct {
	max  atomic.Uint64
	stop func() uint64
}

func atomicHeapSampler(_ *testing.B) *heapPeakSampler {
	s := &heapPeakSampler{}
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		const interval = 5 * time.Millisecond
		t := time.NewTicker(interval)
		defer t.Stop()
		var ms runtime.MemStats
		for {
			select {
			case <-done:
				close(stopped)

				return
			case <-t.C:
				runtime.ReadMemStats(&ms)
				cur := ms.HeapAlloc
				for {
					old := s.max.Load()
					if cur <= old || s.max.CompareAndSwap(old, cur) {
						break
					}
				}
			}
		}
	}()
	s.stop = func() uint64 {
		close(done)
		<-stopped

		return s.max.Load()
	}

	return s
}

func safeSubPipelineBench(a, b uint64) uint64 {
	if a < b {
		return 0
	}

	return a - b
}

func dirBytesPipelineBench(b *testing.B, dir string) int64 {
	b.Helper()
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}

		return nil
	})
	if err != nil {
		b.Fatalf("walk: %v", err)
	}

	return total
}
