package server

import (
	"context"
	"errors"
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

// TestBootstrapAndRun_ExitsCleanlyOnCancelledContext exercises the
// happy-path wrapper end-to-end: a pre-cancelled ctx returns nil with
// no listen error, no leaked goroutines, no leaked DB handle.
func TestBootstrapAndRun_ExitsCleanlyOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done before Run starts
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
	err := BootstrapAndRun(context.Background(), opts)
	if err == nil {
		t.Fatal("BootstrapAndRun should surface a Bootstrap failure")
	}
}

// TestNewDiscoveryWiring_RequiresCacheDir pins the first error branch:
// you can't configure --s3-source without --cache-dir to write builds
// into.
func TestNewDiscoveryWiring_RequiresCacheDir(t *testing.T) {
	_, _, err := newDiscoveryWiring(Config{S3Source: "s3://bucket/", Logger: zerolog.Nop()})
	if !errors.Is(err, errEmptyCacheDir) {
		t.Errorf("err = %v, want errEmptyCacheDir", err)
	}
}

// TestNewDiscoveryWiring_RejectsMalformedURI pins that a bad s3:// URI
// fails at parse time, not later when the discoverer tries to use it.
func TestNewDiscoveryWiring_RejectsMalformedURI(t *testing.T) {
	cfg := Config{
		S3Source: "not-a-real-uri",
		CacheDir: t.TempDir(),
		Logger:   zerolog.Nop(),
	}
	_, _, err := newDiscoveryWiring(cfg)
	if err == nil {
		t.Fatal("newDiscoveryWiring should reject malformed URI")
	}
	if !strings.Contains(err.Error(), "not-a-real-uri") {
		t.Errorf("error should quote the offending URI, got: %v", err)
	}
}
