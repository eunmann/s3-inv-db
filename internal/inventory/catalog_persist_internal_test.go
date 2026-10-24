package inventory

import (
	"context"
	"sync"
	"testing"
)

// rowStore keeps the last row written per inventory.
type rowStore struct {
	rows map[ID]Info
	mu   sync.Mutex
}

func (s *rowStore) Upsert(_ context.Context, info Info) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[info.ID] = info

	return nil
}

func (s *rowStore) Delete(_ context.Context, id ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rows, id)

	return nil
}

// Changes are persisted after c.mu is released, so two changes to one
// inventory can reach the store in either order. Whatever the order,
// the store must end up holding the newest change: an Unload staged
// before a Load but written after it left a loaded run persisted as
// not loaded, and a stale upsert after a Remove resurrected the row.
func TestCatalog_PersistKeepsNewestChange(t *testing.T) {
	const id ID = "src/inv/run"
	loaded := Info{ID: id, State: StateLoaded}
	notLoaded := Info{ID: id, State: StateNotLoaded}
	cases := []struct {
		name      string
		stage     func(c *Catalog) (older, newer pendingWrite)
		newerLast bool
		wantRow   bool
		wantState State
	}{
		{"newer upsert first", stageUpserts(notLoaded, loaded), false, true, StateLoaded},
		{"in order", stageUpserts(notLoaded, loaded), true, true, StateLoaded},
		{"newer delete first", stageUpsertThenDelete(loaded), false, false, ""},
		{"delete in order", stageUpsertThenDelete(loaded), true, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			store := &rowStore{rows: map[ID]Info{}}
			c := NewCatalog(nil)
			defer c.Close()
			c.store = store

			older, newer := tc.stage(c)
			order := []pendingWrite{newer, older}
			if tc.newerLast {
				order = []pendingWrite{older, newer}
			}
			for _, w := range order {
				if err := c.persist(ctx, w); err != nil {
					t.Fatalf("persist seq %d: %v", w.seq, err)
				}
			}

			row, ok := store.rows[id]
			if ok != tc.wantRow || row.State != tc.wantState {
				t.Errorf("store row = %+v (present %v), want present %v state %q", row, ok, tc.wantRow, tc.wantState)
			}
		})
	}
}

func stageUpserts(older, newer Info) func(*Catalog) (pendingWrite, pendingWrite) {
	return func(c *Catalog) (pendingWrite, pendingWrite) {
		c.mu.Lock()
		defer c.mu.Unlock()

		return c.stageLocked(older), c.stageLocked(newer)
	}
}

func stageUpsertThenDelete(older Info) func(*Catalog) (pendingWrite, pendingWrite) {
	return func(c *Catalog) (pendingWrite, pendingWrite) {
		c.mu.Lock()
		defer c.mu.Unlock()

		return c.stageLocked(older), c.stageDeleteLocked(older.ID)
	}
}
