package benchutil

import (
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

const longBenchEnv = "S3INV_LONG_BENCH"

// LongBenchEnabled reports whether the long-bench gate is set. Use
// when a benchmark wants to widen its size table without skipping the
// short variants.
func LongBenchEnabled() bool {
	return os.Getenv(longBenchEnv) != ""
}

// SkipIfNoLongBench skips the benchmark unless the long-bench gate is
// set. Use to gate benchmarks that are too expensive for the default
// `make test` budget.
func SkipIfNoLongBench(b *testing.B) {
	b.Helper()
	if !LongBenchEnabled() {
		b.Skip("set " + longBenchEnv + "=1 to run scaling benchmark")
	}
}

// SilenceZerolog disables global zerolog output for the duration of
// the benchmark. Many bench paths log progress through zerolog; under
// `-benchtime` that floods stderr and skews the measurement.
func SilenceZerolog(b *testing.B) {
	b.Helper()
	prev := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(prev) })
}

// HeapPeakSampler polls runtime.MemStats on a fixed cadence and
// tracks the maximum HeapAlloc observed between Start and Stop.
type HeapPeakSampler struct {
	max     atomic.Uint64
	done    chan struct{}
	stopped chan struct{}
}

const heapSamplerInterval = 5 * time.Millisecond

// StartHeapPeakSampler begins sampling in a background goroutine.
// Call Stop to return the peak HeapAlloc observed.
func StartHeapPeakSampler() *HeapPeakSampler {
	s := &HeapPeakSampler{
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *HeapPeakSampler) run() {
	t := time.NewTicker(heapSamplerInterval)
	defer t.Stop()
	var ms runtime.MemStats
	for {
		select {
		case <-s.done:
			close(s.stopped)
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
}

// Stop terminates sampling, waits for the sampler to drain, and
// returns the peak HeapAlloc.
func (s *HeapPeakSampler) Stop() uint64 {
	close(s.done)
	<-s.stopped
	return s.max.Load()
}

// DirBytes walks dir and returns the total size of all regular files.
// Fails the benchmark if the walk errors.
func DirBytes(b *testing.B, dir string) int64 {
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

// SafeSubU64 returns a-b clamped to zero. Used to defang
// non-monotonic MemStats readings between two snapshots.
func SafeSubU64(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}
