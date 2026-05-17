package budget_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/budget"
)

func TestTracker_AddRemove(t *testing.T) {
	tr := budget.New(1000, 0)
	tr.Add(200)
	tr.Add(300)
	if got := tr.Used(); got != 500 {
		t.Errorf("Used = %d, want 500", got)
	}
	tr.Remove(200)
	if got := tr.Used(); got != 300 {
		t.Errorf("Used after remove = %d, want 300", got)
	}
}

func TestTracker_RemoveClampsAtZero(t *testing.T) {
	tr := budget.New(1000, 0)
	tr.Add(100)
	tr.Remove(9999)
	if got := tr.Used(); got != 0 {
		t.Errorf("Used should clamp at 0, got %d", got)
	}
}

func TestTracker_ReserveRelease(t *testing.T) {
	tr := budget.New(1000, 0)
	if err := tr.Reserve("load-1", 400); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if got := tr.Reserved(); got != 400 {
		t.Errorf("Reserved = %d, want 400", got)
	}
	if got := tr.Available(); got != 600 {
		t.Errorf("Available = %d, want 600", got)
	}
	tr.Release("load-1")
	if got := tr.Reserved(); got != 0 {
		t.Errorf("Reserved after release = %d, want 0", got)
	}
}

func TestTracker_OverBudget(t *testing.T) {
	tr := budget.New(1000, 0)
	tr.Add(800)
	if err := tr.Reserve("load-1", 300); !errors.Is(err, budget.ErrOverBudget) {
		t.Errorf("Reserve over budget = %v, want budget.ErrOverBudget", err)
	}
}

func TestTracker_HeadroomShrinksAvailable(t *testing.T) {
	tr := budget.New(1000, 200)
	if got := tr.Available(); got != 800 {
		t.Errorf("Available with 200 headroom = %d, want 800", got)
	}
	if err := tr.Reserve("ok", 800); err != nil {
		t.Errorf("Reserve up to Available should succeed, got %v", err)
	}
	if err := tr.Reserve("eats-headroom", 1); !errors.Is(err, budget.ErrOverBudget) {
		t.Errorf("Reserve past headroom must fail, got %v", err)
	}
}

func TestTracker_ZeroCapPassesThrough(t *testing.T) {
	// No --max-index-disk: the tracker becomes a no-op so manual
	// loads aren't blocked by an unconfigured budget.
	tr := budget.New(0, 0)
	if err := tr.Reserve("anything", 1); err != nil {
		t.Errorf("Zero-cap tracker should pass reservations through, got %v", err)
	}
	tr.Release("anything")
	if got := tr.Reserved(); got != 0 {
		t.Errorf("Reserved bytes should remain 0 with zero cap, got %d", got)
	}
}

func TestTracker_DuplicateReservationErrors(t *testing.T) {
	tr := budget.New(1000, 0)
	if err := tr.Reserve("dup", 100); err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	if err := tr.Reserve("dup", 100); err == nil {
		t.Error("duplicate Reserve must error")
	}
}

func TestTracker_ReleaseUnknownTokenIsNoOp(t *testing.T) {
	tr := budget.New(1000, 0)
	tr.Release("not-a-thing") // must not panic
	if got := tr.Reserved(); got != 0 {
		t.Errorf("Reserved should remain 0, got %d", got)
	}
}

func TestMeasureDir(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.bin"), make([]byte, 1024), 0o600); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b.bin"), make([]byte, 512), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := budget.MeasureDir(context.Background(), root)
	if err != nil {
		t.Fatalf("MeasureDir: %v", err)
	}
	if got != 1536 {
		t.Errorf("MeasureDir = %d, want 1536", got)
	}
}

func TestMeasureDir_MissingPathIsZero(t *testing.T) {
	got, err := budget.MeasureDir(context.Background(), "/definitely/does/not/exist")
	if err != nil {
		t.Fatalf("MeasureDir on missing path: %v", err)
	}
	if got != 0 {
		t.Errorf("expected 0 bytes, got %d", got)
	}
}
