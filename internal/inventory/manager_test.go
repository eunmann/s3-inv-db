package inventory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

func TestManagerRegister(t *testing.T) {
	m := NewManager()
	defer m.Close()

	err := m.Register("test-id", "Test Inventory", "/path/to/index")
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	info, ok := m.Get("test-id")
	if !ok {
		t.Fatal("Get returned not found")
	}

	if info.ID != "test-id" {
		t.Errorf("ID = %q, want %q", info.ID, "test-id")
	}
	if info.Name != "Test Inventory" {
		t.Errorf("Name = %q, want %q", info.Name, "Test Inventory")
	}
	if info.Path != "/path/to/index" {
		t.Errorf("Path = %q, want %q", info.Path, "/path/to/index")
	}
	if info.State != StateNotLoaded {
		t.Errorf("State = %q, want %q", info.State, StateNotLoaded)
	}
}

func TestManagerRegisterDuplicate(t *testing.T) {
	m := NewManager()
	defer m.Close()

	err := m.Register("test-id", "Test Inventory", "/path/to/index")
	if err != nil {
		t.Fatalf("First Register failed: %v", err)
	}

	err = m.Register("test-id", "Another Name", "/another/path")
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("Second Register error = %v, want %v", err, ErrAlreadyExists)
	}
}

func TestManagerGetNotFound(t *testing.T) {
	m := NewManager()
	defer m.Close()

	_, ok := m.Get("nonexistent")
	if ok {
		t.Error("Get returned ok=true for nonexistent inventory")
	}
}

func TestManagerList(t *testing.T) {
	m := NewManager()
	defer m.Close()

	// Empty list
	list := m.List()
	if len(list) != 0 {
		t.Errorf("List() length = %d, want 0", len(list))
	}

	// Add some inventories
	m.Register("id1", "Inventory 1", "/path/1")
	m.Register("id2", "Inventory 2", "/path/2")

	list = m.List()
	if len(list) != 2 {
		t.Errorf("List() length = %d, want 2", len(list))
	}
}

func TestManagerLoadNotFound(t *testing.T) {
	m := NewManager()
	defer m.Close()

	err := m.Load(context.Background(), "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Load error = %v, want %v", err, ErrNotFound)
	}
}

func TestManagerUnloadNotFound(t *testing.T) {
	m := NewManager()
	defer m.Close()

	err := m.Unload("nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Unload error = %v, want %v", err, ErrNotFound)
	}
}

func TestManagerRemove(t *testing.T) {
	m := NewManager()
	defer m.Close()

	m.Register("test-id", "Test Inventory", "/path/to/index")

	err := m.Remove("test-id")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	_, ok := m.Get("test-id")
	if ok {
		t.Error("Get returned ok=true after Remove")
	}
}

func TestManagerRemoveNotFound(t *testing.T) {
	m := NewManager()
	defer m.Close()

	err := m.Remove("nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Remove error = %v, want %v", err, ErrNotFound)
	}
}

func TestManagerWithIndexNotLoaded(t *testing.T) {
	m := NewManager()
	defer m.Close()

	_ = m.Register("test-id", "Test Inventory", "/path/to/index")

	err := m.WithIndex("test-id", func(*indexread.Index) error { return nil })
	if !errors.Is(err, ErrNotLoaded) {
		t.Errorf("WithIndex error = %v, want %v", err, ErrNotLoaded)
	}
}

func TestManagerWithIndexNotFound(t *testing.T) {
	m := NewManager()
	defer m.Close()

	err := m.WithIndex("nonexistent", func(*indexread.Index) error { return nil })
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("WithIndex error = %v, want %v", err, ErrNotFound)
	}
}

func TestManagerWithTwoIndexes_NotFound(t *testing.T) {
	m := NewManager()
	defer m.Close()
	_ = m.Register("a", "A", "/p")

	err := m.WithTwoIndexes("a", "b", func(*indexread.Index, *indexread.Index) error { return nil })
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("WithTwoIndexes(a, missing) error = %v, want ErrNotFound", err)
	}
}

func TestManagerWithTwoIndexes_NotLoaded(t *testing.T) {
	m := NewManager()
	defer m.Close()
	_ = m.Register("a", "A", "/p")
	_ = m.Register("b", "B", "/q")

	err := m.WithTwoIndexes("a", "b", func(*indexread.Index, *indexread.Index) error { return nil })
	if !errors.Is(err, ErrNotLoaded) {
		t.Errorf("WithTwoIndexes neither loaded error = %v, want ErrNotLoaded", err)
	}
}

// TestManagerConcurrent_RegisterListRemove stress-tests concurrent
// access to the Manager. With -race it asserts there is no data race
// between Register, List, Get, WithIndex, Unload, and Remove. Load
// itself isn't exercised here because it requires a real index on
// disk — see internal/handlers integration tests for that.
func TestManagerConcurrent_RegisterListRemove(t *testing.T) {
	m := NewManager()
	defer m.Close()

	const workers = 16
	const ops = 100

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		go func(workerID int) {
			defer wg.Done()
			for i := range ops {
				id := ID(fmt.Sprintf("w%d-i%d", workerID, i))
				_ = m.Register(id, "name", "/path")
				_, _ = m.Get(id)
				_ = m.List()
				_ = m.WithIndex(id, func(*indexread.Index) error { return nil })
				_ = m.Unload(id)
				_ = m.Remove(id)
			}
		}(w)
	}
	wg.Wait()

	if len(m.List()) != 0 {
		t.Errorf("after stress, List length = %d, want 0", len(m.List()))
	}
}

// TestManagerConcurrent_LoadRemoveRace targets the specific window in
// Manager.Load where the lock is released while indexread.Open runs.
// A racing Remove during that window must not corrupt state and must
// leave the loaded index closed.
func TestManagerConcurrent_LoadRemoveRace(t *testing.T) {
	m := NewManager()
	defer m.Close()

	// Use a bogus path so Open fails quickly — we're testing the
	// state transitions, not a real index load.
	const id = "racy"
	for range 50 {
		if err := m.Register(id, "n", "/no/such/path"); err != nil {
			t.Fatalf("register: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = m.Load(context.Background(), id)
		}()
		go func() {
			defer wg.Done()
			_ = m.Remove(id)
		}()
		wg.Wait()

		// Remove always succeeds (the entry exists when it acquires the
		// lock; Load never deletes), so the inventory must be gone once
		// both goroutines have joined. If it's still present, Remove was
		// somehow undone — that's a real bug.
		if _, ok := m.Get(id); ok {
			info, _ := m.Get(id)
			t.Fatalf("inventory %q still present after race; state=%q", id, info.State)
		}
	}
}
