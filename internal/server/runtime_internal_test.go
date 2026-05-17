package server

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

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
	err := BootstrapAndRun(context.Background(), opts)
	if err == nil {
		t.Fatal("BootstrapAndRun should surface a Bootstrap failure")
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
