package cli

import (
	"os"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/membudget"
)

func TestRunNoArgs(t *testing.T) {
	err := Run(nil)
	if err == nil {
		t.Fatal("expected error with no args")
	}
	if !strings.Contains(err.Error(), "usage") {
		t.Errorf("expected usage message, got: %v", err)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	err := Run([]string{"unknown"})
	if err == nil {
		t.Fatal("expected error with unknown command")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("expected 'unknown command' error, got: %v", err)
	}
}

func TestBuildMissingOut(t *testing.T) {
	err := Run([]string{"build", "--s3-manifest", "s3://bucket/manifest.json"})
	if err == nil {
		t.Fatal("expected error with missing --out")
	}
	if !strings.Contains(err.Error(), "--out") {
		t.Errorf("expected '--out' error, got: %v", err)
	}
}

func TestBuildMissingS3Manifest(t *testing.T) {
	err := Run([]string{"build", "--out", "/out"})
	if err == nil {
		t.Fatal("expected error with missing --s3-manifest")
	}
	if !strings.Contains(err.Error(), "--s3-manifest") {
		t.Errorf("expected '--s3-manifest' error, got: %v", err)
	}
}

func TestQueryMissingIndex(t *testing.T) {
	err := Run([]string{"query", "--prefix", "test/"})
	if err == nil {
		t.Fatal("expected error with missing --index")
	}
	if !strings.Contains(err.Error(), "--index") {
		t.Errorf("expected '--index' error, got: %v", err)
	}
}

func TestQueryMissingPrefix(t *testing.T) {
	err := Run([]string{"query", "--index", "/path/to/index"})
	if err == nil {
		t.Fatal("expected error with missing --prefix")
	}
	if !strings.Contains(err.Error(), "--prefix") {
		t.Errorf("expected '--prefix' error, got: %v", err)
	}
}

func TestDetermineMemoryBudgetCLI(t *testing.T) {
	// CLI flag takes priority
	budget, err := determineMemoryBudget("4GiB")
	if err != nil {
		t.Fatalf("determineMemoryBudget error: %v", err)
	}
	if budget.Total() != 4*1024*1024*1024 {
		t.Errorf("Total() = %d, want %d", budget.Total(), 4*1024*1024*1024)
	}
	if budget.Source() != membudget.BudgetSourceCLI {
		t.Errorf("Source() = %s, want %s", budget.Source(), membudget.BudgetSourceCLI)
	}
}

func TestDetermineMemoryBudgetEnv(t *testing.T) {
	// Set environment variable
	os.Setenv("S3INV_MEM_BUDGET", "2GiB")
	defer os.Unsetenv("S3INV_MEM_BUDGET")

	// Empty CLI should use env var
	budget, err := determineMemoryBudget("")
	if err != nil {
		t.Fatalf("determineMemoryBudget error: %v", err)
	}
	if budget.Total() != 2*1024*1024*1024 {
		t.Errorf("Total() = %d, want %d", budget.Total(), 2*1024*1024*1024)
	}
	if budget.Source() != membudget.BudgetSourceEnv {
		t.Errorf("Source() = %s, want %s", budget.Source(), membudget.BudgetSourceEnv)
	}
}

func TestDetermineMemoryBudgetCLIOverridesEnv(t *testing.T) {
	// Set environment variable
	os.Setenv("S3INV_MEM_BUDGET", "2GiB")
	defer os.Unsetenv("S3INV_MEM_BUDGET")

	// CLI should take priority over env
	budget, err := determineMemoryBudget("8GiB")
	if err != nil {
		t.Fatalf("determineMemoryBudget error: %v", err)
	}
	if budget.Total() != 8*1024*1024*1024 {
		t.Errorf("Total() = %d, want %d", budget.Total(), 8*1024*1024*1024)
	}
	if budget.Source() != membudget.BudgetSourceCLI {
		t.Errorf("Source() = %s, want %s", budget.Source(), membudget.BudgetSourceCLI)
	}
}

func TestDetermineMemoryBudgetDefault(t *testing.T) {
	// Clear env var
	os.Unsetenv("S3INV_MEM_BUDGET")

	// No CLI, no env should use system RAM detection
	budget, err := determineMemoryBudget("")
	if err != nil {
		t.Fatalf("determineMemoryBudget error: %v", err)
	}
	// Should use auto detection
	if budget.Source() != membudget.BudgetSourceAuto50Pct && budget.Source() != membudget.BudgetSourceDefault {
		t.Errorf("Source() = %s, want auto-50pct or default", budget.Source())
	}
}

func TestDetermineMemoryBudgetInvalidCLI(t *testing.T) {
	_, err := determineMemoryBudget("invalid")
	if err == nil {
		t.Fatal("expected error with invalid CLI budget")
	}
	if !strings.Contains(err.Error(), "--mem-budget") {
		t.Errorf("expected '--mem-budget' in error, got: %v", err)
	}
}

func TestDetermineMemoryBudgetInvalidEnv(t *testing.T) {
	os.Setenv("S3INV_MEM_BUDGET", "badvalue")
	defer os.Unsetenv("S3INV_MEM_BUDGET")

	_, err := determineMemoryBudget("")
	if err == nil {
		t.Fatal("expected error with invalid env budget")
	}
	if !strings.Contains(err.Error(), "S3INV_MEM_BUDGET") {
		t.Errorf("expected 'S3INV_MEM_BUDGET' in error, got: %v", err)
	}
}
