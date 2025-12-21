// Package memdiag provides memory diagnostics for debugging memory usage.
//
// Enable debug logging with S3INV_MEM_DEBUG=1
// Enable pprof server with S3INV_MEM_PPROF=1 (listens on :6060)
package memdiag

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	// Registers pprof handlers on DefaultServeMux for the pprof HTTP server.
	_ "net/http/pprof"

	"github.com/eunmann/s3-inv-db/pkg/logging"
)

// Config holds configuration for memory diagnostics.
type Config struct {
	// Enabled controls whether memory diagnostics are active.
	Enabled bool

	// PprofEnabled controls whether pprof server is started.
	PprofEnabled bool

	// LogInterval is the interval for periodic memory logging.
	LogInterval time.Duration
}

// DefaultConfig returns the default configuration, reading from environment.
func DefaultConfig() Config {
	return Config{
		Enabled:      os.Getenv("S3INV_MEM_DEBUG") == "1",
		PprofEnabled: os.Getenv("S3INV_MEM_PPROF") == "1",
		LogInterval:  5 * time.Second,
	}
}

// Stats holds memory statistics from runtime.
type Stats struct {
	// Alloc is bytes allocated and still in use.
	Alloc uint64

	// TotalAlloc is cumulative bytes allocated (even if freed).
	TotalAlloc uint64

	// Sys is bytes obtained from OS.
	Sys uint64

	// HeapAlloc is bytes allocated on heap.
	HeapAlloc uint64

	// HeapSys is bytes obtained from OS for heap.
	HeapSys uint64

	// HeapInuse is bytes in in-use spans.
	HeapInuse uint64

	// HeapIdle is bytes in idle (unused) spans.
	HeapIdle uint64

	// HeapReleased is bytes released to OS.
	HeapReleased uint64

	// StackInuse is bytes used by stack allocator.
	StackInuse uint64

	// NumGC is the number of completed GC cycles.
	NumGC uint32

	// GCCPUFraction is the fraction of CPU used by GC.
	GCCPUFraction float64
}

// Read reads current memory statistics.
func Read() Stats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return Stats{
		Alloc:         m.Alloc,
		TotalAlloc:    m.TotalAlloc,
		Sys:           m.Sys,
		HeapAlloc:     m.HeapAlloc,
		HeapSys:       m.HeapSys,
		HeapInuse:     m.HeapInuse,
		HeapIdle:      m.HeapIdle,
		HeapReleased:  m.HeapReleased,
		StackInuse:    m.StackInuse,
		NumGC:         m.NumGC,
		GCCPUFraction: m.GCCPUFraction,
	}
}

// FormatMB formats bytes as megabytes.
func FormatMB(b uint64) string {
	return fmt.Sprintf("%.1fMB", float64(b)/(1024*1024))
}

// FormatGB formats bytes as gigabytes.
func FormatGB(b uint64) string {
	return fmt.Sprintf("%.2fGB", float64(b)/(1024*1024*1024))
}

// Tracker tracks memory usage over time with periodic logging.
type Tracker struct {
	config   Config
	stopCh   chan struct{}
	doneCh   chan struct{}
	started  atomic.Bool
	mu       sync.Mutex
	phase    string
	peakHeap uint64
}

// NewTracker creates a new memory tracker.
func NewTracker(config Config) *Tracker {
	return &Tracker{
		config: config,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
		phase:  "init",
	}
}

// Start begins periodic memory logging if enabled.
func (t *Tracker) Start() {
	if !t.config.Enabled {
		return
	}

	if !t.started.CompareAndSwap(false, true) {
		return // Already started
	}

	log := logging.L()
	log.Info().Msg("memory diagnostics enabled")

	// Start pprof if requested
	if t.config.PprofEnabled {
		go func() {
			log.Info().Str("addr", ":6060").Msg("starting pprof server")
			if err := http.ListenAndServe(":6060", nil); err != nil {
				log.Error().Err(err).Msg("pprof server failed")
			}
		}()
	}

	// Start periodic logging
	go t.logLoop()
}

// Stop stops the tracker.
func (t *Tracker) Stop() {
	if !t.started.Load() {
		return
	}
	close(t.stopCh)
	<-t.doneCh
}

// SetPhase sets the current phase for logging context.
func (t *Tracker) SetPhase(phase string) {
	t.mu.Lock()
	t.phase = phase
	t.mu.Unlock()

	if t.config.Enabled {
		t.LogNow("phase_change")
	}
}

// LogNow logs current memory stats immediately.
func (t *Tracker) LogNow(reason string) {
	if !t.config.Enabled {
		return
	}

	stats := Read()
	log := logging.L()

	t.mu.Lock()
	phase := t.phase
	if stats.HeapAlloc > t.peakHeap {
		t.peakHeap = stats.HeapAlloc
	}
	peakHeap := t.peakHeap
	t.mu.Unlock()

	log.Debug().
		Str("reason", reason).
		Str("phase", phase).
		Str("heap_alloc", FormatMB(stats.HeapAlloc)).
		Str("heap_sys", FormatMB(stats.HeapSys)).
		Str("heap_inuse", FormatMB(stats.HeapInuse)).
		Str("heap_idle", FormatMB(stats.HeapIdle)).
		Str("stack_inuse", FormatMB(stats.StackInuse)).
		Str("sys_total", FormatMB(stats.Sys)).
		Str("peak_heap", FormatMB(peakHeap)).
		Uint32("num_gc", stats.NumGC).
		Float64("gc_cpu_pct", stats.GCCPUFraction*100).
		Msg("memory stats")
}

// LogWithBudget logs memory stats along with budget comparison.
func (t *Tracker) LogWithBudget(reason string, budgetInUse, budgetTotal uint64) {
	if !t.config.Enabled {
		return
	}

	stats := Read()
	log := logging.L()

	t.mu.Lock()
	phase := t.phase
	if stats.HeapAlloc > t.peakHeap {
		t.peakHeap = stats.HeapAlloc
	}
	peakHeap := t.peakHeap
	t.mu.Unlock()

	// Calculate divergence between budget tracking and actual heap
	var divergence float64
	if budgetInUse > 0 {
		divergence = float64(stats.HeapAlloc) / float64(budgetInUse)
	}

	log.Debug().
		Str("reason", reason).
		Str("phase", phase).
		Str("heap_alloc", FormatMB(stats.HeapAlloc)).
		Str("heap_sys", FormatMB(stats.HeapSys)).
		Str("budget_inuse", FormatMB(budgetInUse)).
		Str("budget_total", FormatMB(budgetTotal)).
		Float64("heap_vs_budget_ratio", divergence).
		Str("peak_heap", FormatMB(peakHeap)).
		Uint32("num_gc", stats.NumGC).
		Msg("memory stats with budget")

	// Warn if heap is significantly larger than budget tracking
	if divergence > 2.0 && budgetInUse > 100*1024*1024 {
		log.Warn().
			Str("heap_alloc", FormatMB(stats.HeapAlloc)).
			Str("budget_inuse", FormatMB(budgetInUse)).
			Float64("ratio", divergence).
			Msg("MEMORY WARNING: heap usage significantly exceeds budget tracking")
	}
}

// PeakHeap returns the peak heap allocation seen.
func (t *Tracker) PeakHeap() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.peakHeap
}

// logLoop runs the periodic logging.
func (t *Tracker) logLoop() {
	defer close(t.doneCh)

	ticker := time.NewTicker(t.config.LogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			t.LogNow("shutdown")
			return
		case <-ticker.C:
			t.LogNow("periodic")
		}
	}
}

// Global tracker for convenience.
var globalTracker *Tracker
var globalOnce sync.Once

// Global returns the global tracker, initializing if needed.
func Global() *Tracker {
	globalOnce.Do(func() {
		globalTracker = NewTracker(DefaultConfig())
	})
	return globalTracker
}

// StartGlobal starts the global tracker.
func StartGlobal() {
	Global().Start()
}

// StopGlobal stops the global tracker.
func StopGlobal() {
	if globalTracker != nil {
		globalTracker.Stop()
	}
}

// ForceGC forces a garbage collection and logs the result.
func ForceGC() {
	log := logging.L()

	before := Read()
	runtime.GC()
	after := Read()

	freed := int64(before.HeapAlloc) - int64(after.HeapAlloc)

	log.Debug().
		Str("before_heap", FormatMB(before.HeapAlloc)).
		Str("after_heap", FormatMB(after.HeapAlloc)).
		Str("freed", FormatMB(uint64(max(freed, 0)))).
		Msg("forced GC")
}
