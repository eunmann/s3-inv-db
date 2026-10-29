package inventory_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// hydrateIntent registers id not loaded, with the given pin and
// user-unload mark.
func hydrateIntent(t *testing.T, m *inventory.Catalog, id inventory.ID, pinned bool, unloadedAt time.Time) {
	t.Helper()
	if err := m.Hydrate(t.Context(), inventory.Info{
		ID: id, Name: "n", Path: "p", State: inventory.StateNotLoaded,
		Pinned: pinned, UserUnloadedAt: unloadedAt,
	}, ""); err != nil {
		t.Fatal(err)
	}
}

// A manual load that ends without its index in place leaves the run's
// pin and user-unload mark as they were before it, in memory and in the
// store, on every way such a load ends.
func TestCatalog_FailedManualLoadKeepsPreLoadIntent(t *testing.T) {
	indexDir := buildMinimalIndex(t)
	cases := map[string]func(cancel context.CancelFunc) inventory.BuildFunc{
		"build error": func(context.CancelFunc) inventory.BuildFunc {
			return func(context.Context, inventory.Info) (string, error) { return "", errFakeLoadFailed }
		},
		"open error": func(context.CancelFunc) inventory.BuildFunc {
			empty := t.TempDir()

			return func(context.Context, inventory.Info) (string, error) { return empty, nil }
		},
		"cancelled build": func(cancel context.CancelFunc) inventory.BuildFunc {
			return func(ctx context.Context, _ inventory.Info) (string, error) {
				cancel()

				return "", fmt.Errorf("run pipeline: %w", ctx.Err())
			}
		},
		"cancelled after build": func(cancel context.CancelFunc) inventory.BuildFunc {
			return func(context.Context, inventory.Info) (string, error) {
				cancel()

				return indexDir, nil
			}
		},
	}
	unloadedAt := time.Unix(1_700_000_000, 0)
	for name, build := range cases {
		for _, pinned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pinned=%v", name, pinned), func(t *testing.T) {
				m, store := openManagerWithStore(t)
				const id = "src/inv/run1"
				hydrateIntent(t, m, id, pinned, unloadedAt)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if err := m.LoadWith(ctx, id, build(cancel)); err == nil {
					t.Fatal("LoadWith succeeded, want an error")
				}
				got, _ := m.Get(id)
				persisted, err := store.Get(t.Context(), id)
				if err != nil {
					t.Fatal(err)
				}
				for where, info := range map[string]inventory.Info{"catalog": got, "store": persisted} {
					if info.State == inventory.StateLoaded {
						t.Errorf("%s: state = loaded after a failed load", where)
					}
					if info.Pinned != pinned {
						t.Errorf("%s: Pinned = %v, want %v as before the load", where, info.Pinned, pinned)
					}
					if !info.UserUnloadedAt.Equal(unloadedAt) {
						t.Errorf("%s: UserUnloadedAt = %v, want %v as before the load", where, info.UserUnloadedAt, unloadedAt)
					}
				}
			})
		}
	}
}

// A manual load that succeeds pins the run and clears its user-unload
// mark; an auto-load leaves both alone.
func TestCatalog_SucceededLoadIntent(t *testing.T) {
	indexDir := buildMinimalIndex(t)
	build := func(context.Context, inventory.Info) (string, error) { return indexDir, nil }
	unloadedAt := time.Unix(1_700_000_000, 0)

	m := inventory.NewCatalog(nil)
	defer m.Close()
	hydrateIntent(t, m, "src/inv/manual", false, unloadedAt)
	if err := m.LoadWith(t.Context(), "src/inv/manual", build); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Get("src/inv/manual"); !got.Pinned || !got.UserUnloadedAt.IsZero() {
		t.Errorf("manual load: Pinned = %v, UserUnloadedAt = %v; want pinned, no mark", got.Pinned, got.UserUnloadedAt)
	}

	hydrateIntent(t, m, "src/inv/auto", false, unloadedAt)
	if err := m.AutoLoad(t.Context(), "src/inv/auto", build); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Get("src/inv/auto"); got.Pinned || !got.UserUnloadedAt.Equal(unloadedAt) {
		t.Errorf("auto-load: Pinned = %v, UserUnloadedAt = %v; want unpinned, mark kept", got.Pinned, got.UserUnloadedAt)
	}
}

// A pin set while a manual load builds is the user's latest word: the
// load's outcome, success or failure, doesn't overwrite it.
func TestCatalog_PinSetDuringManualLoadStands(t *testing.T) {
	indexDir := buildMinimalIndex(t)
	for _, fail := range []bool{false, true} {
		for _, before := range []bool{false, true} {
			t.Run(fmt.Sprintf("fail=%v/pinned=%v", fail, before), func(t *testing.T) {
				m := inventory.NewCatalog(nil)
				defer m.Close()
				const id = "src/inv/run1"
				hydrateIntent(t, m, id, before, time.Time{})
				err := m.LoadWith(t.Context(), id, func(ctx context.Context, _ inventory.Info) (string, error) {
					if err := m.SetPinned(ctx, id, !before); err != nil {
						return "", fmt.Errorf("pin: %w", err)
					}
					if fail {
						return "", errFakeLoadFailed
					}

					return indexDir, nil
				})
				if (err != nil) != fail {
					t.Fatalf("LoadWith = %v, want failure %v", err, fail)
				}
				if got, _ := m.Get(id); got.Pinned != !before {
					t.Errorf("Pinned = %v, want %v as set during the load", got.Pinned, !before)
				}
			})
		}
	}
}
