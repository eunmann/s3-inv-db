package membudget

import (
	"sync"
	"testing"
	"time"
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
	if budget.InUse() != 0 {
		t.Errorf("InUse() = %d, want 0", budget.InUse())
	}
	if budget.Available() != 1000 {
		t.Errorf("Available() = %d, want 1000", budget.Available())
	}
	if budget.Source() != BudgetSourceCLI {
		t.Errorf("Source() = %s, want %s", budget.Source(), BudgetSourceCLI)
	}
}

func TestTryReserve(t *testing.T) {
	budget := New(Config{TotalBytes: 1000})

	// Reserve some memory
	if !budget.TryReserve(400) {
		t.Error("TryReserve(400) should succeed")
	}
	if budget.InUse() != 400 {
		t.Errorf("InUse() = %d, want 400", budget.InUse())
	}

	// Reserve more
	if !budget.TryReserve(500) {
		t.Error("TryReserve(500) should succeed")
	}
	if budget.InUse() != 900 {
		t.Errorf("InUse() = %d, want 900", budget.InUse())
	}

	// Try to exceed budget
	if budget.TryReserve(200) {
		t.Error("TryReserve(200) should fail when only 100 available")
	}
	if budget.InUse() != 900 {
		t.Errorf("InUse() should still be 900 after failed reserve")
	}

	// Reserve exactly remaining
	if !budget.TryReserve(100) {
		t.Error("TryReserve(100) should succeed")
	}
	if budget.InUse() != 1000 {
		t.Errorf("InUse() = %d, want 1000", budget.InUse())
	}
}

func TestRelease(t *testing.T) {
	budget := New(Config{TotalBytes: 1000})

	budget.TryReserve(800)
	budget.Release(300)

	if budget.InUse() != 500 {
		t.Errorf("InUse() = %d, want 500", budget.InUse())
	}
	if budget.Available() != 500 {
		t.Errorf("Available() = %d, want 500", budget.Available())
	}

	// Release more than reserved (should cap at 0)
	budget.Release(600)
	if budget.InUse() != 0 {
		t.Errorf("InUse() = %d, want 0 after over-release", budget.InUse())
	}
}

func TestReserveBlocking(t *testing.T) {
	budget := New(Config{TotalBytes: 1000})
	budget.TryReserve(900) // Only 100 available

	var wg sync.WaitGroup
	wg.Add(1)

	reserved := false
	go func() {
		defer wg.Done()
		// This should block until memory is released
		err := budget.Reserve(200)
		if err != nil {
			t.Errorf("Reserve() error: %v", err)
		}
		reserved = true
	}()

	// Give the goroutine time to block
	time.Sleep(50 * time.Millisecond)
	if reserved {
		t.Error("Reserve should be blocking")
	}

	// Release memory
	budget.Release(200)

	// Wait for goroutine to complete
	wg.Wait()
	if !reserved {
		t.Error("Reserve should have succeeded after release")
	}
}

func TestReserveImpossible(t *testing.T) {
	budget := New(Config{TotalBytes: 100})

	err := budget.Reserve(200)
	if err == nil {
		t.Error("Reserve(200) should fail when budget is 100")
	}
}

func TestReserveWithTimeout(t *testing.T) {
	budget := New(Config{TotalBytes: 1000})
	budget.TryReserve(950) // Only 50 available

	// Should timeout
	start := time.Now()
	ok := budget.ReserveWithTimeout(100, 50*time.Millisecond)
	elapsed := time.Since(start)

	if ok {
		t.Error("ReserveWithTimeout should have failed")
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("Should have waited at least 50ms, waited %v", elapsed)
	}
}

func TestConcurrentReservations(t *testing.T) {
	budget := New(Config{TotalBytes: 10000})

	var wg sync.WaitGroup
	const numGoroutines = 100
	const perReservation = 100

	wg.Add(numGoroutines)
	for range numGoroutines {
		go func() {
			defer wg.Done()
			if budget.TryReserve(perReservation) {
				defer budget.Release(perReservation)
				// Simulate work
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()

	// All reservations should be released
	if budget.InUse() != 0 {
		t.Errorf("InUse() = %d, want 0 after all goroutines complete", budget.InUse())
	}
}

func TestStats(t *testing.T) {
	budget := New(Config{TotalBytes: 1000, Source: BudgetSourceEnv})
	budget.TryReserve(400)

	stats := budget.Stats()

	if stats.TotalBytes != 1000 {
		t.Errorf("TotalBytes = %d, want 1000", stats.TotalBytes)
	}
	if stats.InUseBytes != 400 {
		t.Errorf("InUseBytes = %d, want 400", stats.InUseBytes)
	}
	if stats.AvailableBytes != 600 {
		t.Errorf("AvailableBytes = %d, want 600", stats.AvailableBytes)
	}
	if stats.Source != BudgetSourceEnv {
		t.Errorf("Source = %s, want %s", stats.Source, BudgetSourceEnv)
	}
	if stats.UsagePercent < 39.9 || stats.UsagePercent > 40.1 {
		t.Errorf("UsagePercent = %f, want ~40.0", stats.UsagePercent)
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
}
