//go:build linux

package benchutil

import (
	"encoding/binary"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// HWCounters counts CPU hardware events (cache and TLB misses, cycles,
// instructions) for the calling thread, user space only, between Start
// and Stop. It needs kernel.perf_event_paranoid ≤ 2; when the kernel
// refuses, NewHWCounters returns nil and every method is a no-op, so
// benchmarks still run without the metrics.
//
// The counters follow one OS thread, so the benchmark goroutine is
// locked to it until Close: run the measured code on that goroutine.
type HWCounters struct {
	fds    []int
	names  []string
	totals []uint64
}

type hwEvent struct {
	name   string
	typ    uint32
	config uint64
}

// cacheEvent encodes a PERF_TYPE_HW_CACHE config: cache, op, result.
func cacheEvent(cache, op, result uint64) uint64 { return cache | op<<8 | result<<16 }

// hwEvents lists the counters NewHWCounters opens.
func hwEvents() []hwEvent {
	return []hwEvent{
		{"cycles", unix.PERF_TYPE_HARDWARE, unix.PERF_COUNT_HW_CPU_CYCLES},
		{"instr", unix.PERF_TYPE_HARDWARE, unix.PERF_COUNT_HW_INSTRUCTIONS},
		{"LLCmiss", unix.PERF_TYPE_HW_CACHE, cacheEvent(unix.PERF_COUNT_HW_CACHE_LL, unix.PERF_COUNT_HW_CACHE_OP_READ, unix.PERF_COUNT_HW_CACHE_RESULT_MISS)},
		{"L1Dmiss", unix.PERF_TYPE_HW_CACHE, cacheEvent(unix.PERF_COUNT_HW_CACHE_L1D, unix.PERF_COUNT_HW_CACHE_OP_READ, unix.PERF_COUNT_HW_CACHE_RESULT_MISS)},
		{"dTLBmiss", unix.PERF_TYPE_HW_CACHE, cacheEvent(unix.PERF_COUNT_HW_CACHE_DTLB, unix.PERF_COUNT_HW_CACHE_OP_READ, unix.PERF_COUNT_HW_CACHE_RESULT_MISS)},
	}
}

// corePMUFile names the performance-core PMU on hybrid CPUs (Intel
// P/E cores). Generic events there must name the PMU they count on.
const corePMUFile = "/sys/bus/event_source/devices/cpu_core/type"

// pmuTypeShift is PERF_PMU_TYPE_SHIFT: where a generic event's config
// carries the PMU type.
const pmuTypeShift = 32

// corePMU returns the extended-type bits that select the performance
// cores' PMU, or 0 on a CPU that isn't hybrid.
func corePMU() uint64 {
	data, err := os.ReadFile(corePMUFile)
	if err != nil {
		return 0
	}
	t, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 32)
	if err != nil {
		return 0
	}

	return t << pmuTypeShift
}

// NewHWCounters opens the counters, disabled, on the current thread.
// Events this CPU doesn't support are left out. On a hybrid CPU they
// count on the performance cores only, so pin the benchmark there.
func NewHWCounters(tb testing.TB) *HWCounters {
	tb.Helper()
	runtime.LockOSThread()
	c := &HWCounters{}
	pmu := corePMU()
	for _, ev := range hwEvents() {
		attr := unix.PerfEventAttr{
			Type:   ev.typ,
			Config: ev.config | pmu,
			Size:   uint32(unsafe.Sizeof(unix.PerfEventAttr{})),
			Bits:   unix.PerfBitDisabled | unix.PerfBitExcludeKernel | unix.PerfBitExcludeHv,
		}
		fd, err := unix.PerfEventOpen(&attr, 0, -1, -1, unix.PERF_FLAG_FD_CLOEXEC)
		if err != nil {
			continue
		}
		c.fds = append(c.fds, fd)
		c.names = append(c.names, ev.name)
	}
	if len(c.fds) == 0 {
		runtime.UnlockOSThread()

		return nil
	}
	c.totals = make([]uint64, len(c.fds))

	return c
}

// Start resumes counting.
func (c *HWCounters) Start() {
	if c == nil {
		return
	}
	for _, fd := range c.fds {
		_ = unix.IoctlSetInt(fd, unix.PERF_EVENT_IOC_ENABLE, 0)
	}
}

// Stop pauses counting and adds what was counted since Start.
func (c *HWCounters) Stop() {
	if c == nil {
		return
	}
	var buf [8]byte
	for i, fd := range c.fds {
		_ = unix.IoctlSetInt(fd, unix.PERF_EVENT_IOC_DISABLE, 0)
		if n, err := unix.Read(fd, buf[:]); err == nil && n == len(buf) {
			c.totals[i] += binary.LittleEndian.Uint64(buf[:])
		}
		_ = unix.IoctlSetInt(fd, unix.PERF_EVENT_IOC_RESET, 0)
	}
}

// Report adds every counter, divided by ops, to b's results, plus
// instructions per cycle.
func (c *HWCounters) Report(b *testing.B, ops int) {
	b.Helper()
	if c == nil {
		return
	}
	var cycles, instr uint64
	for i, name := range c.names {
		b.ReportMetric(float64(c.totals[i])/float64(ops), name+"/q")
		switch name {
		case "cycles":
			cycles = c.totals[i]
		case "instr":
			instr = c.totals[i]
		}
	}
	if cycles > 0 {
		b.ReportMetric(float64(instr)/float64(cycles), "IPC")
	}
}

// Close releases the counters and the thread lock.
func (c *HWCounters) Close() {
	if c == nil {
		return
	}
	for _, fd := range c.fds {
		_ = unix.Close(fd)
	}
	runtime.UnlockOSThread()
}
