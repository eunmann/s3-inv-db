package inventory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

var errPrefixMissing = errors.New("prefix missing from index")

// waitUntil polls cond for up to 5s.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// An Unload waiting for a slow reader must close the index it took out,
// not one a Load installed meanwhile: that leaked the old index and left
// the inventory Loaded with no index.
func TestCatalog_UnloadWaitingOnReaderKeepsNewerLoad(t *testing.T) {
	ctx := t.Context()
	c := inventory.NewCatalog(nil)
	defer c.Close()
	const id = "src/inv/run"
	if err := c.Register(ctx, id, "n", buildKeysIndexDir(t, "a/x", "b/y")); err != nil {
		t.Fatal(err)
	}
	if err := c.Load(ctx, id); err != nil {
		t.Fatal(err)
	}

	inReader, release := make(chan struct{}), make(chan struct{})
	readerDone := make(chan error, 1)
	go func() {
		readerDone <- c.WithIndex(id, func(*indexread.Index) error {
			close(inReader)
			<-release

			return nil
		})
	}()
	<-inReader

	unloadDone := make(chan error, 1)
	go func() { unloadDone <- c.Unload(ctx, id) }()
	waitUntil(t, "unload to start", func() bool {
		info, _ := c.Get(id)

		return info.State == inventory.StateNotLoaded
	})
	if err := c.Load(ctx, id); err != nil {
		t.Fatalf("Load during pending unload: %v", err)
	}
	close(release)
	if err := <-readerDone; err != nil {
		t.Fatalf("reader: %v", err)
	}
	if err := <-unloadDone; err != nil {
		t.Fatalf("Unload: %v", err)
	}

	err := c.WithIndex(id, func(idx *indexread.Index) error {
		if _, ok := idx.StatsForPrefix("a/"); !ok {
			return errPrefixMissing
		}

		return nil
	})
	if err != nil {
		t.Fatalf("newer load unusable after the earlier unload finished: %v", err)
	}
}

// blockingStore blocks every write until released.
type blockingStore struct {
	entered chan struct{}
	release chan struct{}
}

func (s *blockingStore) Upsert(context.Context, inventory.Info) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release

	return nil
}

func (s *blockingStore) Delete(context.Context, inventory.ID) error { return nil }

// Persisting a state change must not hold the catalog lock: every query
// takes it, so a slow SQLite write stalled all reads.
func TestCatalog_ReadsDoNotWaitOnStoreWrites(t *testing.T) {
	ctx := t.Context()
	c := inventory.NewCatalog(nil)
	defer c.Close()
	const id = "src/inv/run"
	if err := c.Register(ctx, id, "n", "/unused"); err != nil {
		t.Fatal(err)
	}
	store := &blockingStore{entered: make(chan struct{}, 1), release: make(chan struct{})}
	c.SetMirrorForTest(store)
	defer close(store.release)

	go func() { _ = c.SetPinned(ctx, id, true) }()
	<-store.entered

	read := make(chan struct{})
	go func() {
		_, _ = c.Get(id)
		_ = c.List()
		close(read)
	}()
	select {
	case <-read:
	case <-time.After(2 * time.Second):
		t.Fatal("Get/List blocked behind a store write")
	}
}
