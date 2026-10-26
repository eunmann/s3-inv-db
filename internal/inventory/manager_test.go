package inventory_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

func TestManagerRegister(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	err := m.Register(t.Context(), "test-id", "Test Inventory", "/path/to/index")
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
	if info.State != inventory.StateNotLoaded {
		t.Errorf("State = %q, want %q", info.State, inventory.StateNotLoaded)
	}
}

func TestManagerRegisterDuplicate(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	err := m.Register(t.Context(), "test-id", "Test Inventory", "/path/to/index")
	if err != nil {
		t.Fatalf("First Register failed: %v", err)
	}

	err = m.Register(t.Context(), "test-id", "Another Name", "/another/path")
	if !errors.Is(err, inventory.ErrAlreadyExists) {
		t.Errorf("Second Register error = %v, want %v", err, inventory.ErrAlreadyExists)
	}
}

func TestManagerGetNotFound(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	_, ok := m.Get("nonexistent")
	if ok {
		t.Error("Get returned ok=true for nonexistent inventory")
	}
}

func TestManagerList(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	// Empty list
	list := m.List()
	if len(list) != 0 {
		t.Errorf("List() length = %d, want 0", len(list))
	}

	// Add some inventories
	m.Register(t.Context(), "id1", "Inventory 1", "/path/1")
	m.Register(t.Context(), "id2", "Inventory 2", "/path/2")

	list = m.List()
	if len(list) != 2 {
		t.Errorf("List() length = %d, want 2", len(list))
	}
}

func TestManagerLoadNotFound(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	err := m.Load(t.Context(), "nonexistent")
	if !errors.Is(err, inventory.ErrNotFound) {
		t.Errorf("Load error = %v, want %v", err, inventory.ErrNotFound)
	}
}

func TestManagerUnloadNotFound(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	err := m.Unload(t.Context(), "nonexistent")
	if !errors.Is(err, inventory.ErrNotFound) {
		t.Errorf("Unload error = %v, want %v", err, inventory.ErrNotFound)
	}
}

func TestManagerRemove(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	m.Register(t.Context(), "test-id", "Test Inventory", "/path/to/index")

	err := m.Remove(t.Context(), "test-id")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	_, ok := m.Get("test-id")
	if ok {
		t.Error("Get returned ok=true after Remove")
	}
}

// TestManagerRemoveWhileLoading: a run whose build is in flight can't
// be removed, since the build would publish an index nobody owns; once
// the load ends it can.
func TestManagerRemoveWhileLoading(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()
	if err := m.Register(t.Context(), "test-id", "Test Inventory", "/path/to/index"); err != nil {
		t.Fatalf("Register: %v", err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- m.LoadWith(t.Context(), "test-id", func(context.Context, inventory.Info) (string, error) {
			close(started)
			<-release

			return "", errFakeBuildBroke
		})
	}()
	<-started

	err := m.Remove(t.Context(), "test-id")
	close(release)
	if !errors.Is(err, inventory.ErrInvalidState) {
		t.Errorf("Remove while loading error = %v, want %v", err, inventory.ErrInvalidState)
	}
	if err := <-done; !errors.Is(err, errFakeBuildBroke) {
		t.Fatalf("LoadWith error = %v, want %v", err, errFakeBuildBroke)
	}
	if err := m.Remove(t.Context(), "test-id"); err != nil {
		t.Errorf("Remove after the load ended: %v", err)
	}
}

func TestManagerRemoveNotFound(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	err := m.Remove(t.Context(), "nonexistent")
	if !errors.Is(err, inventory.ErrNotFound) {
		t.Errorf("Remove error = %v, want %v", err, inventory.ErrNotFound)
	}
}

func TestManagerWithIndexNotLoaded(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	_ = m.Register(t.Context(), "test-id", "Test Inventory", "/path/to/index")

	err := m.WithIndex("test-id", func(*indexread.Index) error { return nil })
	if !errors.Is(err, inventory.ErrNotLoaded) {
		t.Errorf("WithIndex error = %v, want %v", err, inventory.ErrNotLoaded)
	}
}

func TestManagerWithIndexNotFound(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	err := m.WithIndex("nonexistent", func(*indexread.Index) error { return nil })
	if !errors.Is(err, inventory.ErrNotFound) {
		t.Errorf("WithIndex error = %v, want %v", err, inventory.ErrNotFound)
	}
}

func TestManagerWithTwoIndexes_NotFound(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()
	_ = m.Register(t.Context(), "a", "A", "/p")

	err := m.WithTwoIndexes(t.Context(), "a", "b", func(*indexread.Index, *indexread.Index) error { return nil })
	if !errors.Is(err, inventory.ErrNotFound) {
		t.Errorf("WithTwoIndexes(a, missing) error = %v, want inventory.ErrNotFound", err)
	}
}

func TestManagerWithTwoIndexes_NotLoaded(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()
	_ = m.Register(t.Context(), "a", "A", "/p")
	_ = m.Register(t.Context(), "b", "B", "/q")

	err := m.WithTwoIndexes(t.Context(), "a", "b", func(*indexread.Index, *indexread.Index) error { return nil })
	if !errors.Is(err, inventory.ErrNotLoaded) {
		t.Errorf("WithTwoIndexes neither loaded error = %v, want inventory.ErrNotLoaded", err)
	}
}

// TestManagerConcurrent_RegisterListRemove stress-tests concurrent
// access to the Catalog. With -race it asserts there is no data race
// between Register, List, Get, WithIndex, Unload, and Remove. Load
// itself isn't exercised here because it requires a real index on
// disk — see internal/handlers integration tests for that.
func TestManagerConcurrent_RegisterListRemove(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	const workers = 16
	const ops = 100

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := range workers {
		go func(workerID int) {
			defer wg.Done()
			for i := range ops {
				id := inventory.ID(fmt.Sprintf("w%d-i%d", workerID, i))
				_ = m.Register(t.Context(), id, "name", "/path")
				_, _ = m.Get(id)
				_ = m.List()
				_ = m.WithIndex(id, func(*indexread.Index) error { return nil })
				_ = m.Unload(t.Context(), id)
				_ = m.Remove(t.Context(), id)
			}
		}(w)
	}
	wg.Wait()

	if len(m.List()) != 0 {
		t.Errorf("after stress, List length = %d, want 0", len(m.List()))
	}
}

// TestManagerConcurrent_LoadRemoveRace targets the specific window in
// Catalog.Load where the lock is released while indexread.Open runs.
// A racing Remove during that window must not corrupt state: it either
// removes the run before the load starts, or is refused while the run
// is loading and succeeds once the load has ended.
func TestManagerConcurrent_LoadRemoveRace(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()

	// Use a bogus path so Open fails quickly — we're testing the
	// state transitions, not a real index load.
	const id = "racy"
	for range 50 {
		if err := m.Register(t.Context(), id, "n", "/no/such/path"); err != nil {
			t.Fatalf("register: %v", err)
		}

		var wg sync.WaitGroup
		var removeErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = m.Load(t.Context(), id)
		}()
		go func() {
			defer wg.Done()
			removeErr = m.Remove(t.Context(), id)
		}()
		wg.Wait()

		switch {
		case errors.Is(removeErr, inventory.ErrInvalidState):
			if err := m.Remove(t.Context(), id); err != nil {
				t.Fatalf("Remove after the load ended: %v", err)
			}
		case removeErr != nil:
			t.Fatalf("Remove: %v", removeErr)
		}
		// Load never deletes and a refused Remove was retried, so the
		// inventory must be gone. If it's still present, Remove was
		// somehow undone — that's a real bug.
		if _, ok := m.Get(id); ok {
			info, _ := m.Get(id)
			t.Fatalf("inventory %q still present after race; state=%q", id, info.State)
		}
	}
}

// TestCatalog_OnChangeSeesEveryPersistedChange: SetOnChange hears about
// changes made outside a job too (pin, remove), which is how the UI's
// live stream learns of them.
func TestCatalog_OnChangeSeesEveryPersistedChange(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()
	var mu sync.Mutex
	var seen []inventory.ID
	m.SetOnChange(func(id inventory.ID) {
		mu.Lock()
		seen = append(seen, id)
		mu.Unlock()
	})

	if err := m.Register(t.Context(), "src/inv/run", "n", "/p"); err != nil {
		t.Fatal(err)
	}
	if err := m.SetPinned(t.Context(), "src/inv/run", true); err != nil {
		t.Fatal(err)
	}
	if err := m.Remove(t.Context(), "src/inv/run"); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("onChange saw %v, want register, pin and remove", seen)
	}
	for _, id := range seen {
		if id != "src/inv/run" {
			t.Errorf("onChange got %q", id)
		}
	}
}

// TestCatalog_CancelledLoadIsNotAnError: a build the user cancels leaves
// the run not loaded, not in error with the context's error text.
func TestCatalog_CancelledLoadIsNotAnError(t *testing.T) {
	m := inventory.NewCatalog(nil)
	defer m.Close()
	if err := m.Register(t.Context(), "test-id", "n", "/p"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	err := m.LoadWith(ctx, "test-id", func(ctx context.Context, _ inventory.Info) (string, error) {
		cancel()

		return "", fmt.Errorf("run pipeline: %w", ctx.Err())
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadWith error = %v, want context.Canceled", err)
	}
	info, _ := m.Get("test-id")
	if info.State != inventory.StateNotLoaded || info.Error != "" {
		t.Errorf("after cancel: state %s, error %q; want not loaded, no error", info.State, info.Error)
	}
}
