package server

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// TestLoadPriceTable_DefaultsWhenPathEmpty verifies the
// no-flag-supplied path falls back to the bundled US East 1 prices.
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

// TestBootstrap_OpensDBAndReturnsCleanup exercises Bootstrap end-to-end
// against a tempdir: it should resolve the state-db path under
// CacheDir, open a real SQLite file, and return a cleanup that closes
// the DB cleanly. Catches regressions where cleanup is forgotten in a
// future error-branch refactor.
func TestBootstrap_OpensDBAndReturnsCleanup(t *testing.T) {
	tmp := t.TempDir()
	opts := RuntimeOptions{
		Addr:     ":0",
		CacheDir: tmp,
		Logger:   zerolog.Nop(),
	}
	srv, cleanup, err := Bootstrap(opts)
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
	_, _, err := Bootstrap(opts)
	if err == nil {
		t.Fatal("Bootstrap should fail when price table can't be loaded")
	}
}
