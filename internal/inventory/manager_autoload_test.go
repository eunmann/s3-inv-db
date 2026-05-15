package inventory_test

import (
	"errors"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

func TestManagerSetPinned(t *testing.T) {
	m := inventory.NewManager()
	const id = "src/inv/runA"
	if err := m.Register(id, "n", "p"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if info, _ := m.Get(id); info.Pinned {
		t.Fatal("freshly registered run should not be pinned")
	}
	if err := m.SetPinned(id, true); err != nil {
		t.Fatalf("SetPinned true: %v", err)
	}
	if info, _ := m.Get(id); !info.Pinned {
		t.Error("Pinned not set after SetPinned(true)")
	}
	if err := m.SetPinned(id, false); err != nil {
		t.Fatalf("SetPinned false: %v", err)
	}
	if info, _ := m.Get(id); info.Pinned {
		t.Error("Pinned not cleared after SetPinned(false)")
	}
}

func TestManagerSetPinned_NotFound(t *testing.T) {
	m := inventory.NewManager()
	if err := m.SetPinned("nope", true); !errors.Is(err, inventory.ErrNotFound) {
		t.Errorf("SetPinned on unknown id = %v, want inventory.ErrNotFound", err)
	}
}

func TestManagerRecordAutoLoadFailure(t *testing.T) {
	m := inventory.NewManager()
	const id = "src/inv/run1"
	if err := m.Register(id, "n", "p"); err != nil {
		t.Fatal(err)
	}
	retry := time.Now().Add(5 * time.Minute)
	if err := m.RecordAutoLoadFailure(id, "boom", retry); err != nil {
		t.Fatalf("RecordAutoLoadFailure: %v", err)
	}
	info, _ := m.Get(id)
	if info.AutoLoadFailureCount != 1 {
		t.Errorf("count = %d, want 1", info.AutoLoadFailureCount)
	}
	if info.Error != "boom" {
		t.Errorf("Error = %q, want boom", info.Error)
	}
	if !info.AutoLoadBackoffUntil.Equal(retry) {
		t.Errorf("BackoffUntil = %v, want %v", info.AutoLoadBackoffUntil, retry)
	}

	// Subsequent failure increments, not doubles.
	if err := m.RecordAutoLoadFailure(id, "again", retry); err != nil {
		t.Fatalf("second RecordAutoLoadFailure: %v", err)
	}
	if info, _ := m.Get(id); info.AutoLoadFailureCount != 2 {
		t.Errorf("count after 2nd failure = %d, want 2", info.AutoLoadFailureCount)
	}
}

func TestManagerTouchAccessed_InMemoryOnly(t *testing.T) {
	m := inventory.NewManager()
	const id = "src/inv/run1"
	if err := m.Register(id, "n", "p"); err != nil {
		t.Fatal(err)
	}
	before, _ := m.Get(id)
	if !before.LastAccessedAt.IsZero() {
		t.Fatal("LastAccessedAt should start zero")
	}
	m.TouchAccessed(id)
	after, _ := m.Get(id)
	if after.LastAccessedAt.IsZero() {
		t.Error("LastAccessedAt should be set after TouchAccessed")
	}
	// Unknown id is a no-op, not a panic.
	m.TouchAccessed("nope")
}

func TestManagerEvictForBudget_PreservesUserUnloadedAt(t *testing.T) {
	// EvictForBudget must NOT stamp UserUnloadedAt — it represents
	// budget eviction, not user intent. The auto-loader is free to
	// reload the run later if a newer one appears.
	m := inventory.NewManager()
	const id = "src/inv/run1"
	if err := m.Hydrate(inventory.Info{
		ID:         id,
		Name:       "n",
		Path:       "p",
		State:      inventory.StateNotLoaded,
		IndexBytes: 1024,
	}, ""); err != nil {
		t.Fatal(err)
	}
	// EvictForBudget on a non-loaded inventory returns ErrInvalidState,
	// which is the documented behaviour — callers (loadgate) ignore it.
	err := m.EvictForBudget(id)
	if err == nil {
		t.Fatal("EvictForBudget on non-loaded inventory should error")
	}
	info, _ := m.Get(id)
	if !info.UserUnloadedAt.IsZero() {
		t.Errorf("EvictForBudget set UserUnloadedAt=%v; budget eviction must not stamp it", info.UserUnloadedAt)
	}
}
