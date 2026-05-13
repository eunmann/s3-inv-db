package inventory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
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
	if info.State != StatePending {
		t.Errorf("State = %q, want %q", info.State, StatePending)
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

func TestManagerGetIndexNotLoaded(t *testing.T) {
	m := NewManager()
	defer m.Close()

	_ = m.Register("test-id", "Test Inventory", "/path/to/index")

	_, err := m.GetIndex("test-id")
	if !errors.Is(err, ErrNotLoaded) {
		t.Errorf("GetIndex error = %v, want %v", err, ErrNotLoaded)
	}
}

func TestManagerGetIndexNotFound(t *testing.T) {
	m := NewManager()
	defer m.Close()

	_, err := m.GetIndex("nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetIndex error = %v, want %v", err, ErrNotFound)
	}
}

// TestManagerConcurrent_RegisterListRemove stress-tests concurrent
// access to the Manager. With -race it asserts there is no data race
// between Register, List, Get, GetIndex, Unload, and Remove. Load
// itself isn't exercised here because it requires a real index on
// disk — see internal/handlers integration tests for that.
func TestManagerConcurrent_RegisterListRemove(t *testing.T) {
	m := NewManager()
	defer m.Close()

	const workers = 16
	const ops = 100

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				id := fmt.Sprintf("w%d-i%d", workerID, i)
				_ = m.Register(id, "name", "/path")
				_, _ = m.Get(id)
				_ = m.List()
				_, _ = m.GetIndex(id)
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
	for i := 0; i < 50; i++ {
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

		// Cleanup: whichever order ran, the inventory should either be
		// gone (Remove won) or in StateError (Load won and Open failed).
		info, ok := m.Get(id)
		if ok && info.State != StateError && info.State != StateParsing {
			t.Fatalf("unexpected post-race state: %q", info.State)
		}
		_ = m.Remove(id)
	}
}
