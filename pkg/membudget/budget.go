// Package membudget provides memory budget configuration for the index build pipeline.
//
// The budget provides configuration values that guide memory usage decisions
// across pipeline stages (aggregators, run buffers, merge buffers, index build).
// Memory limits are enforced via heap monitoring in the aggregator, not via
// explicit reservation tracking.
package membudget

import (
	"errors"
	"fmt"

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

// Budget holds memory budget configuration for the pipeline.
// Budget values are used to guide memory-related decisions like flush thresholds
// and buffer sizes. Actual memory enforcement is done via heap monitoring.
//
// Budget is safe for concurrent read access.
type Budget struct {
	total  uint64 // Total allowed bytes
	source BudgetSource
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
	return &Budget{
		total:  cfg.TotalBytes,
		source: cfg.Source,
	}
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

// Source returns how the budget was determined.
func (b *Budget) Source() BudgetSource {
	return b.source
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
