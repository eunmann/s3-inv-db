package server

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/testsupport/dbtest"
	"github.com/rs/zerolog"
)

func TestLoadPriceTable_DefaultsWhenPathEmpty(t *testing.T) {
	pt, err := loadPriceTable("", zerolog.Nop())
	if err != nil {
		t.Fatalf("loadPriceTable: %v", err)
	}
	if len(pt.PerGBMonth) == 0 {
		t.Errorf("default price table has no per-GB rates: %+v", pt)
	}
}

func TestLoadPriceTable_MissingFile(t *testing.T) {
	_, err := loadPriceTable("/no/such/file.json", zerolog.Nop())
	if err == nil {
		t.Fatal("loadPriceTable should error on missing file")
	}
	if !strings.Contains(err.Error(), "/no/such/file.json") {
		t.Errorf("error should mention the path: %v", err)
	}
}

func TestBootstrap_OpensDBAndReturnsCleanup(t *testing.T) {
	tmp := t.TempDir()
	opts := RuntimeOptions{
		Addr:     ":0",
		CacheDir: tmp,
		Logger:   zerolog.Nop(),
	}
	srv, cleanup, err := Bootstrap(t.Context(), opts)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	defer cleanup()
	if srv == nil {
		t.Fatal("Bootstrap returned nil server")
	}
	wantPath := filepath.Join(tmp, "state.db")
	if _, err := filepath.Abs(wantPath); err != nil {
		t.Errorf("expected DB at %s: %v", wantPath, err)
	}
}

func TestBootstrap_PropagatesPriceTableLoadError(t *testing.T) {
	opts := RuntimeOptions{
		Addr:           ":0",
		CacheDir:       t.TempDir(),
		PriceTablePath: "/definitely/not/here.json",
		Logger:         zerolog.Nop(),
	}
	_, _, err := Bootstrap(t.Context(), opts)
	if err == nil {
		t.Fatal("Bootstrap should fail when price table can't be loaded")
	}
}

func TestBootstrapAndRun_ExitsCleanlyOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	opts := RuntimeOptions{
		Addr:     ":0",
		CacheDir: t.TempDir(),
		Logger:   zerolog.Nop(),
	}
	if err := BootstrapAndRun(ctx, opts); err != nil {
		t.Errorf("BootstrapAndRun with cancelled ctx = %v, want nil", err)
	}
}

func TestBootstrapAndRun_PropagatesBootstrapError(t *testing.T) {
	opts := RuntimeOptions{
		Addr:           ":0",
		CacheDir:       t.TempDir(),
		PriceTablePath: "/no/such/price-table.json",
		Logger:         zerolog.Nop(),
	}
	err := BootstrapAndRun(t.Context(), opts)
	if err == nil {
		t.Fatal("BootstrapAndRun should surface a Bootstrap failure")
	}
}

// TestApplyInventoryConfigs_DoesNotFallthroughOnTransientGetError
// guards against the previous bug where any non-nil error from
// ConfigStore.Get was treated as "not found" and the function fell
// through to Upsert, which then succeeded silently or failed with a
// confusing unique-key error. The store closes mid-call: Get must
// return a real error (not ErrStoreNotFound), and applyInventoryConfigs
// must surface it as a Get failure, not attempt an Upsert.
func TestApplyInventoryConfigs_DoesNotFallthroughOnTransientGetError(t *testing.T) {
	db := dbtest.OpenMemDB(t)
	store := inventory.NewConfigStore(db)
	// Close the DB so subsequent operations return "database is closed".
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	err := applyInventoryConfigs(t.Context(), store, []InventoryConfigEntry{
		{Source: "src", Name: "name", AutoLoad: true},
	})
	if err == nil {
		t.Fatal("applyInventoryConfigs should return an error when Get fails on a closed DB")
	}
	// The error must blame the Get-time read, not an attempted insert,
	// confirming we did not fall through to Upsert on a transient error.
	if !strings.Contains(err.Error(), "get ") && !strings.Contains(err.Error(), "lookup ") {
		t.Errorf("error = %v; expected a Get/lookup error, not an Upsert error (fallthrough regression)", err)
	}
}

func TestNewDiscoveryWiring_RequiresCacheDir(t *testing.T) {
	_, err := newDiscoveryWiring(t.Context(), Config{S3Source: "s3://bucket/", Logger: zerolog.Nop()})
	if !errors.Is(err, errEmptyCacheDir) {
		t.Errorf("err = %v, want errEmptyCacheDir", err)
	}
}

func TestNewDiscoveryWiring_RejectsMalformedURI(t *testing.T) {
	cfg := Config{
		S3Source: "not-a-real-uri",
		CacheDir: t.TempDir(),
		Logger:   zerolog.Nop(),
	}
	_, err := newDiscoveryWiring(t.Context(), cfg)
	if err == nil {
		t.Fatal("newDiscoveryWiring should reject malformed URI")
	}
	if !strings.Contains(err.Error(), "not-a-real-uri") {
		t.Errorf("error should quote the offending URI, got: %v", err)
	}
}
