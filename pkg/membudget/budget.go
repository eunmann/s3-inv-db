// Package membudget provides a central memory budget manager for the index build pipeline.
//
// The budget ensures that total memory usage stays within configured limits,
// coordinating across multiple concurrent consumers (aggregators, run buffers,
// merge buffers, index build).
package membudget

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/sysmem"
)

// DefaultBudgetBytes is the fallback memory budget when system RAM cannot be detected.
// 8 GB is a conservative default for most systems.
const DefaultBudgetBytes uint64 = 8 * 1024 * 1024 * 1024

// BudgetSource indicates how the memory budget was determined.
type BudgetSource string

const (
	// BudgetSourceAuto50Pct indicates the budget was set to 50% of detected RAM.
	BudgetSourceAuto50Pct BudgetSource = "auto-50pct"
	// BudgetSourceDefault indicates the budget used the fallback default.
	BudgetSourceDefault BudgetSource = "default"
	// BudgetSourceCLI indicates the budget was set via CLI flag.
	BudgetSourceCLI BudgetSource = "cli"
	// BudgetSourceEnv indicates the budget was set via environment variable.
	BudgetSourceEnv BudgetSource = "env"
)

// Budget manages memory allocations across the pipeline.
// It provides a soft enforcement mechanism where callers request memory
// before allocating and release it when done.
//
// Budget is safe for concurrent use.
type Budget struct {
	total  uint64 // Total allowed bytes
	inUse  atomic.Uint64
	source BudgetSource

	// For blocking reservations
	mu   sync.Mutex
	cond *sync.Cond
}

// Config holds configuration for creating a Budget.
type Config struct {
	// TotalBytes is the total memory budget in bytes.
	// If 0, it will be calculated from system RAM.
	TotalBytes uint64

	// Source indicates how the budget was determined.
	Source BudgetSource
}

// New creates a new Budget with the given configuration.
func New(cfg Config) *Budget {
	b := &Budget{
		total:  cfg.TotalBytes,
		source: cfg.Source,
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// NewFromSystemRAM creates a Budget set to 50% of system RAM.
// If RAM cannot be detected, uses DefaultBudgetBytes.
func NewFromSystemRAM() *Budget {
	result := sysmem.Total()

	var total uint64
	var source BudgetSource

	if result.Reliable {
		total = result.TotalBytes / 2 // 50% of RAM
		source = BudgetSourceAuto50Pct
	} else {
		total = DefaultBudgetBytes
		source = BudgetSourceDefault
	}

	return New(Config{
		TotalBytes: total,
		Source:     source,
	})
}

// Total returns the total budget in bytes.
func (b *Budget) Total() uint64 {
	return b.total
}

// InUse returns the currently reserved bytes.
func (b *Budget) InUse() uint64 {
	return b.inUse.Load()
}

// Available returns the available bytes (total - inUse).
func (b *Budget) Available() uint64 {
	inUse := b.inUse.Load()
	if inUse >= b.total {
		return 0
	}
	return b.total - inUse
}

// Source returns how the budget was determined.
func (b *Budget) Source() BudgetSource {
	return b.source
}

// TryReserve attempts to reserve n bytes.
// Returns true if successful, false if it would exceed the budget.
//
// This is a non-blocking operation.
func (b *Budget) TryReserve(n uint64) bool {
	for {
		current := b.inUse.Load()
		newTotal := current + n
		if newTotal > b.total {
			return false
		}
		if b.inUse.CompareAndSwap(current, newTotal) {
			return true
		}
		// CAS failed, retry
	}
}

// Reserve blocks until n bytes can be reserved.
// Returns an error if the reservation is impossible (n > total).
//
// This uses a condition variable to avoid busy-waiting.
func (b *Budget) Reserve(n uint64) error {
	if n > b.total {
		return fmt.Errorf("reservation of %d bytes exceeds total budget of %d bytes", n, b.total)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for !b.tryReserveLocked(n) {
		b.cond.Wait()
	}
	return nil
}

// ReserveWithTimeout blocks until n bytes can be reserved or timeout expires.
// Returns true if reserved, false if timeout occurred.
func (b *Budget) ReserveWithTimeout(n uint64, timeout time.Duration) bool {
	if n > b.total {
		return false
	}

	deadline := time.Now().Add(timeout)

	b.mu.Lock()
	defer b.mu.Unlock()

	for !b.tryReserveLocked(n) {
		if time.Now().After(deadline) {
			return false
		}
		// Use a short sleep instead of full wait to check timeout
		b.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		b.mu.Lock()
	}
	return true
}

// tryReserveLocked attempts reservation while holding the lock.
func (b *Budget) tryReserveLocked(n uint64) bool {
	current := b.inUse.Load()
	newTotal := current + n
	if newTotal > b.total {
		return false
	}
	b.inUse.Store(newTotal)
	return true
}

// Release returns n bytes to the available pool.
// Must be called when reserved memory is no longer needed.
func (b *Budget) Release(n uint64) {
	for {
		current := b.inUse.Load()
		if n > current {
			// Prevent underflow - cap at 0
			if b.inUse.CompareAndSwap(current, 0) {
				break
			}
		} else {
			if b.inUse.CompareAndSwap(current, current-n) {
				break
			}
		}
	}

	// Signal waiters that memory was released
	b.cond.Broadcast()
}

// Stats returns current budget statistics.
type Stats struct {
	TotalBytes     uint64
	InUseBytes     uint64
	AvailableBytes uint64
	Source         BudgetSource
	UsagePercent   float64
}

// Stats returns current budget statistics.
func (b *Budget) Stats() Stats {
	inUse := b.inUse.Load()
	available := uint64(0)
	if inUse < b.total {
		available = b.total - inUse
	}
	usagePct := float64(inUse) / float64(b.total) * 100.0
	return Stats{
		TotalBytes:     b.total,
		InUseBytes:     inUse,
		AvailableBytes: available,
		Source:         b.source,
		UsagePercent:   usagePct,
	}
}

// Allocation fractions for different pipeline stages.
// These help coordinate memory usage across concurrent operations.
const (
	// FractionAggregator is the fraction of budget for aggregator memory.
	FractionAggregator = 0.50

	// FractionRunBuffers is the fraction for run file read/write buffers.
	FractionRunBuffers = 0.20

	// FractionMerge is the fraction for merge phase buffers.
	FractionMerge = 0.15

	// FractionIndexBuild is the fraction for index building and MPHF.
	FractionIndexBuild = 0.10

	// FractionHeadroom is reserved headroom for misc allocations.
	FractionHeadroom = 0.05
)

// AggregatorBudget returns the budget allocated for aggregators.
func (b *Budget) AggregatorBudget() uint64 {
	return uint64(float64(b.total) * FractionAggregator)
}

// RunBufferBudget returns the budget for run file buffers.
func (b *Budget) RunBufferBudget() uint64 {
	return uint64(float64(b.total) * FractionRunBuffers)
}

// MergeBudget returns the budget for merge phase.
func (b *Budget) MergeBudget() uint64 {
	return uint64(float64(b.total) * FractionMerge)
}

// IndexBuildBudget returns the budget for index building.
func (b *Budget) IndexBuildBudget() uint64 {
	return uint64(float64(b.total) * FractionIndexBuild)
}

// ParseHumanSize parses a human-readable size string (e.g., "4GiB", "512MB").
// Supported suffixes: B, KB, KiB, MB, MiB, GB, GiB, TB, TiB.
func ParseHumanSize(s string) (uint64, error) {
	if s == "" {
		return 0, errors.New("empty size string")
	}

	// Find where the number ends
	numEnd := 0
	for i, c := range s {
		if (c < '0' || c > '9') && c != '.' {
			numEnd = i
			break
		}
		numEnd = i + 1
	}

	numStr := s[:numEnd]
	suffix := s[numEnd:]

	var num float64
	if _, err := fmt.Sscanf(numStr, "%f", &num); err != nil {
		return 0, fmt.Errorf("invalid number: %s", numStr)
	}

	var multiplier float64
	switch suffix {
	case "", "B":
		multiplier = 1.0
	case "KB":
		multiplier = 1000
	case "KiB", "K":
		multiplier = 1024
	case "MB":
		multiplier = 1000 * 1000
	case "MiB", "M":
		multiplier = 1024 * 1024
	case "GB":
		multiplier = 1000 * 1000 * 1000
	case "GiB", "G":
		multiplier = 1024 * 1024 * 1024
	case "TB":
		multiplier = 1000 * 1000 * 1000 * 1000
	case "TiB", "T":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size suffix: %s", suffix)
	}

	return uint64(num * multiplier), nil
}

// FormatBytes formats a byte count as a human-readable string.
func FormatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
