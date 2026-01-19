package inventory

import (
	"context"
	"errors"
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
