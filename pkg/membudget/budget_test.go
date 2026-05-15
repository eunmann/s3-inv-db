package membudget_test

import (
	"errors"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/membudget"
)

func TestBudgetBasic(t *testing.T) {
	budget := membudget.New(membudget.Config{
		TotalBytes: 1000,
		Source:     membudget.BudgetSourceCLI,
	})

	if budget.Total() != 1000 {
		t.Errorf("Total() = %d, want 1000", budget.Total())
	}
	if budget.Source() != membudget.BudgetSourceCLI {
		t.Errorf("Source() = %s, want %s", budget.Source(), membudget.BudgetSourceCLI)
	}
}

func TestNewFromSystemRAM(t *testing.T) {
	budget := membudget.NewFromSystemRAM()

	if budget.Total() < 1024*1024*1024 {
		t.Logf("Budget is %d bytes", budget.Total())
	}

	if budget.Source() != membudget.BudgetSourceAuto50Pct && budget.Source() != membudget.BudgetSourceDefault {
		t.Errorf("Source = %s, want auto-50pct or default", budget.Source())
	}
}

func TestParseHumanSize(t *testing.T) {
	tests := []struct {
		input   string
		want    uint64
		wantErr error
	}{
		{"1024", 1024, nil},
		{"100B", 100, nil},
		{"1KB", 1000, nil},
		{"1KiB", 1024, nil},
		{"1K", 1024, nil},
		{"1MB", 1000000, nil},
		{"1MiB", 1024 * 1024, nil},
		{"1M", 1024 * 1024, nil},
		{"1GB", 1000000000, nil},
		{"1GiB", 1024 * 1024 * 1024, nil},
		{"4GiB", 4 * 1024 * 1024 * 1024, nil},
		{"0.5GiB", 512 * 1024 * 1024, nil},
		{"", 0, membudget.ErrEmptySize},
		{"XYZ", 0, membudget.ErrInvalidSizeNumber},
		{"100XB", 0, membudget.ErrUnknownSizeSuffix},
	}

	for _, tt := range tests {
		got, err := membudget.ParseHumanSize(tt.input)
		if tt.wantErr != nil {
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("ParseHumanSize(%q) err = %v, want %v", tt.input, err, tt.wantErr)
			}

			continue
		}
		if err != nil {
			t.Errorf("ParseHumanSize(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ParseHumanSize(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestFractionBudgets(t *testing.T) {
	budget := membudget.New(membudget.Config{TotalBytes: 10000})

	// Check fractions add up to approximately 1.
	total := membudget.FractionAggregator + membudget.FractionRunBuffers + membudget.FractionMerge +
		membudget.FractionIndexBuild + membudget.FractionHeadroom
	if total < 0.99 || total > 1.01 {
		t.Errorf("Fractions sum to %f, want ~1.0", total)
	}

	if budget.AggregatorBudget() != 5000 {
		t.Errorf("AggregatorBudget() = %d, want 5000", budget.AggregatorBudget())
	}
	if budget.RunBufferBudget() != 2000 {
		t.Errorf("RunBufferBudget() = %d, want 2000", budget.RunBufferBudget())
	}
	if budget.MergeBudget() != 1500 {
		t.Errorf("MergeBudget() = %d, want 1500", budget.MergeBudget())
	}
	if budget.IndexBuildBudget() != 1000 {
		t.Errorf("IndexBuildBudget() = %d, want 1000", budget.IndexBuildBudget())
	}
}
