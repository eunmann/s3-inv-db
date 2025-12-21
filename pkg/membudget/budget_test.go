package membudget

import (
	"testing"
)

func TestBudgetBasic(t *testing.T) {
	budget := New(Config{
		TotalBytes: 1000,
		Source:     BudgetSourceCLI,
	})

	// Verify initial state
	if budget.Total() != 1000 {
		t.Errorf("Total() = %d, want 1000", budget.Total())
	}
	if budget.Source() != BudgetSourceCLI {
		t.Errorf("Source() = %s, want %s", budget.Source(), BudgetSourceCLI)
	}
}

func TestNewFromSystemRAM(t *testing.T) {
	budget := NewFromSystemRAM()

	// Should have a reasonable budget
	if budget.Total() < 1024*1024*1024 { // At least 1GB
		t.Logf("Budget is %d bytes (%s)", budget.Total(), FormatBytes(budget.Total()))
	}

	// Source should be auto or default
	if budget.Source() != BudgetSourceAuto50Pct && budget.Source() != BudgetSourceDefault {
		t.Errorf("Source = %s, want auto-50pct or default", budget.Source())
	}
}

func TestParseHumanSize(t *testing.T) {
	tests := []struct {
		input   string
		want    uint64
		wantErr bool
	}{
		{"1024", 1024, false},
		{"100B", 100, false},
		{"1KB", 1000, false},
		{"1KiB", 1024, false},
		{"1K", 1024, false},
		{"1MB", 1000000, false},
		{"1MiB", 1024 * 1024, false},
		{"1M", 1024 * 1024, false},
		{"1GB", 1000000000, false},
		{"1GiB", 1024 * 1024 * 1024, false},
		{"4GiB", 4 * 1024 * 1024 * 1024, false},
		{"0.5GiB", 512 * 1024 * 1024, false},
		{"", 0, true},
		{"XYZ", 0, true},
		{"100XB", 0, true},
	}

	for _, tt := range tests {
		got, err := ParseHumanSize(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseHumanSize(%q) should error", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("ParseHumanSize(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ParseHumanSize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.00 KiB"},
		{1536, "1.50 KiB"},
		{1024 * 1024, "1.00 MiB"},
		{1024 * 1024 * 1024, "1.00 GiB"},
		{4 * 1024 * 1024 * 1024, "4.00 GiB"},
	}

	for _, tt := range tests {
		got := FormatBytes(tt.input)
		if got != tt.want {
			t.Errorf("FormatBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFractionBudgets(t *testing.T) {
	budget := New(Config{TotalBytes: 10000})

	// Check fractions add up to approximately 1
	total := FractionAggregator + FractionRunBuffers + FractionMerge +
		FractionIndexBuild + FractionHeadroom
	if total < 0.99 || total > 1.01 {
		t.Errorf("Fractions sum to %f, want ~1.0", total)
	}

	// Check individual budgets
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
