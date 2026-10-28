//go:build linux

package benchutil_test

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"golang.org/x/sys/unix"
)

// TestProcStatsCountsEveryThread: CPU time spent on other goroutines'
// threads counts, as a parallel build's does.
func TestProcStatsCountsEveryThread(t *testing.T) {
	before := benchutil.ReadProcStats(t)
	var wg sync.WaitGroup
	for range 2 {
		// Each goroutine spins on its own thread until that thread has
		// used 100ms of CPU, however busy the machine is.
		wg.Go(func() {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			start, err := threadCPU()
			for err == nil {
				var now time.Duration
				if now, err = threadCPU(); now-start >= 100*time.Millisecond {
					return
				}
			}
			t.Errorf("getrusage: %v", err)
		})
	}
	wg.Wait()
	d := benchutil.ReadProcStats(t).Sub(before)
	if cpu := d.User + d.Sys; cpu < 200*time.Millisecond {
		t.Errorf("two threads using 100ms of CPU each counted %v, want at least 200ms", cpu)
	}
}

// threadCPU returns the calling thread's CPU time.
func threadCPU() (time.Duration, error) {
	var ru unix.Rusage
	err := unix.Getrusage(unix.RUSAGE_THREAD, &ru)

	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()), err
}

// TestProcStatsAddSub: Add totals spans field by field and Sub undoes it.
func TestProcStatsAddSub(t *testing.T) {
	a := benchutil.ProcStats{MinorFaults: 1, MajorFaults: 2, ReadBytes: 3, User: 4, Sys: 5, WriteBytes: 6}
	b := benchutil.ProcStats{MinorFaults: 10, MajorFaults: 20, ReadBytes: 30, User: 40, Sys: 50, WriteBytes: 60}
	sum := a.Add(b)
	want := benchutil.ProcStats{MinorFaults: 11, MajorFaults: 22, ReadBytes: 33, User: 44, Sys: 55, WriteBytes: 66}
	if sum != want {
		t.Errorf("Add = %+v, want %+v", sum, want)
	}
	if got := sum.Sub(b); got != a {
		t.Errorf("Sub = %+v, want %+v", got, a)
	}
}
