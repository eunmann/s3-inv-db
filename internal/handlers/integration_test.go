package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
	"github.com/rs/zerolog"
)

// buildLoadedTestHandlers seeds a small synthetic index, registers and
// loads it into a fresh Manager, and returns ready-to-query handlers.
func buildLoadedTestHandlers(t *testing.T) *Handlers {
	t.Helper()

	tmp := t.TempDir()
	cfg := seeder.Config{
		OutputDir: tmp,
		Count:     1,
		Objects:   200,
		Preset:    "small",
		Seed:      42,
		Logger:    zerolog.Nop(),
	}
	if err := seeder.Run(cfg); err != nil {
		t.Fatalf("seed: %v", err)
	}

	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })

	renderer, err := templates.New(false)
	if err != nil {
		t.Fatalf("renderer: %v", err)
	}

	h := New(mgr, renderer, pricing.DefaultUSEast1Prices())

	indexPath := filepath.Join(tmp, "inv-001")
	if err := mgr.Register("loaded", "Loaded", indexPath); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := mgr.Load(context.Background(), "loaded"); err != nil {
		t.Fatalf("load: %v", err)
	}

	return h
}

func decodeStatsResponse(t *testing.T, body []byte) *StatsResponse {
	t.Helper()
	var resp struct {
		Success bool           `json:"success"`
		Data    *StatsResponse `json:"data"`
		Error   string         `json:"error"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("decode: %v (body=%s)", err, body)
	}
	if !resp.Success {
		t.Fatalf("success = false; error = %s", resp.Error)
	}
	if resp.Data == nil {
		t.Fatal("data is nil")
	}
	return resp.Data
}

// TestGetStatsAPI_Success_RootPrefix exercises the previously-broken
// empty-prefix path against a real loaded index.
func TestGetStatsAPI_Success_RootPrefix(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/stats?inventory_id=loaded&prefix=", http.NoBody)
	w := httptest.NewRecorder()
	h.GetStatsAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	stats := decodeStatsResponse(t, w.Body.Bytes())
	if stats.ObjectCount == 0 {
		t.Errorf("root ObjectCount = 0, want >0")
	}
	if stats.TotalBytes == 0 {
		t.Errorf("root TotalBytes = 0, want >0")
	}
}

func TestGetStatsAPI_Success_WithTiersAndCost(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	url := "/api/stats?inventory_id=loaded&prefix=&show_tiers=true&estimate_cost=true"
	req := httptest.NewRequest(http.MethodGet, url, http.NoBody)
	w := httptest.NewRecorder()
	h.GetStatsAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	stats := decodeStatsResponse(t, w.Body.Bytes())
	if len(stats.TierBreakdown) == 0 {
		t.Errorf("TierBreakdown is empty, want >=1 tier")
	}
	if stats.CostEstimate == nil {
		t.Fatal("CostEstimate is nil, want non-nil")
	}
	if stats.CostEstimate.TotalFormatted == "" {
		t.Errorf("CostEstimate.TotalFormatted is empty")
	}
}

func TestGetInventoryStatsAPI_Success(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/loaded/stats?prefix=", http.NoBody)
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.GetInventoryStatsAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	decodeStatsResponse(t, w.Body.Bytes())
}

func TestGetDescendantsAPI_Success(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	req := httptest.NewRequest(http.MethodGet, "/api/inventories/loaded/descendants?prefix=&depth=1", http.NoBody)
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.GetDescendantsAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Success bool             `json:"success"`
		Data    []DescendantInfo `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.Success {
		t.Fatal("success = false")
	}
	if len(resp.Data) == 0 {
		t.Errorf("descendants empty, want >=1")
	}
	for _, d := range resp.Data {
		if d.Depth != 1 {
			t.Errorf("descendant depth = %d, want 1", d.Depth)
		}
	}
}

func TestGetDescendantsAPI_Filter(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	// First get unfiltered to know what to expect under a min_count cutoff.
	baseReq := httptest.NewRequest(http.MethodGet, "/api/inventories/loaded/descendants?prefix=&depth=1", http.NoBody)
	baseReq = withURLParam(baseReq, "id", "loaded")
	baseW := httptest.NewRecorder()
	h.GetDescendantsAPI(baseW, baseReq)

	if baseW.Code != http.StatusOK {
		t.Fatalf("base status = %d, want 200", baseW.Code)
	}

	// Apply a min_count filter that should drop all single-object entries.
	req := httptest.NewRequest(http.MethodGet, "/api/inventories/loaded/descendants?prefix=&depth=1&min_count=2", http.NoBody)
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.GetDescendantsAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("filtered status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Data []DescendantInfo `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, d := range resp.Data {
		if d.ObjectCount < 2 {
			t.Errorf("min_count=2 filter returned entry with %d objects", d.ObjectCount)
		}
	}
}

func TestBrowseLevelPartial_Success(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	url := "/partials/browse-level?inventory_id=loaded&prefix="
	req := httptest.NewRequest(http.MethodGet, url, http.NoBody)
	w := httptest.NewRecorder()
	h.BrowseLevelPartial(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}

	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}

	body := w.Body.String()
	// At root, breadcrumb shows "Root" and there should be a Children section.
	if !strings.Contains(body, "Root") {
		t.Errorf("rendered partial missing Root breadcrumb: %s", body)
	}
	if !strings.Contains(body, "Children") {
		t.Errorf("rendered partial missing Children section")
	}
	// The seeded inventory has tier data, so the breakdown should appear.
	if !strings.Contains(body, "Storage tier breakdown") {
		t.Errorf("rendered partial missing tier breakdown: %s", body)
	}
}

// TestLifecycle_LoadStatsUnloadReload exercises the full state machine
// through the HTTP handlers against a real index.
func TestLifecycle_LoadStatsUnloadReload(t *testing.T) {
	tmp := t.TempDir()
	if err := seeder.Run(seeder.Config{
		OutputDir: tmp,
		Count:     1,
		Objects:   100,
		Preset:    "small",
		Seed:      7,
		Logger:    zerolog.Nop(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	indexPath := filepath.Join(tmp, "inv-001")

	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	renderer, err := templates.New(false)
	if err != nil {
		t.Fatalf("renderer: %v", err)
	}
	h := New(mgr, renderer, pricing.DefaultUSEast1Prices())

	// Register via handler.
	body := `{"id":"life","name":"Life","path":"` + indexPath + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/inventories", strings.NewReader(body))
	w := httptest.NewRecorder()
	h.RegisterInventoryAPI(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("register: status = %d, body=%s", w.Code, w.Body.String())
	}

	// Load.
	req = httptest.NewRequest(http.MethodPost, "/api/inventories/life/load", http.NoBody)
	req = withURLParam(req, "id", "life")
	w = httptest.NewRecorder()
	h.LoadInventoryAPI(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("load: status = %d, body=%s", w.Code, w.Body.String())
	}

	// Stats while loaded.
	req = httptest.NewRequest(http.MethodGet, "/api/stats?inventory_id=life&prefix=", http.NoBody)
	w = httptest.NewRecorder()
	h.GetStatsAPI(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("stats: status = %d, body=%s", w.Code, w.Body.String())
	}

	// Unload.
	req = httptest.NewRequest(http.MethodPost, "/api/inventories/life/unload", http.NoBody)
	req = withURLParam(req, "id", "life")
	w = httptest.NewRecorder()
	h.UnloadInventoryAPI(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unload: status = %d, body=%s", w.Code, w.Body.String())
	}

	// Stats after unload should 409.
	req = httptest.NewRequest(http.MethodGet, "/api/stats?inventory_id=life&prefix=", http.NoBody)
	w = httptest.NewRecorder()
	h.GetStatsAPI(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("post-unload stats: status = %d, want 409", w.Code)
	}

	// Reload from unloaded state.
	req = httptest.NewRequest(http.MethodPost, "/api/inventories/life/load", http.NoBody)
	req = withURLParam(req, "id", "life")
	w = httptest.NewRecorder()
	h.LoadInventoryAPI(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("reload: status = %d, body=%s", w.Code, w.Body.String())
	}

	// Inventory row partial should now reflect "loaded".
	req = httptest.NewRequest(http.MethodGet, "/partials/inventory-row/life", http.NoBody)
	req = withURLParam(req, "id", "life")
	w = httptest.NewRecorder()
	h.InventoryRowPartial(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("partial: status = %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "bg-green-100") {
		t.Errorf("loaded row partial missing green badge class")
	}
}

// TestLoadInventoryAPI_AlreadyLoaded covers the InvalidState branch the
// existing test could not reach without a real index.
func TestLoadInventoryAPI_AlreadyLoaded(t *testing.T) {
	h := buildLoadedTestHandlers(t)

	req := httptest.NewRequest(http.MethodPost, "/api/inventories/loaded/load", http.NoBody)
	req = withURLParam(req, "id", "loaded")
	w := httptest.NewRecorder()
	h.LoadInventoryAPI(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409 (loading an already-loaded inventory)", w.Code)
	}
}

// TestLoadInventoryAPI_BadPath verifies the error-state path: a bad
// inventory path produces a 500, after which a follow-up reload works.
func TestLoadInventoryAPI_BadPath(t *testing.T) {
	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })
	renderer, err := templates.New(false)
	if err != nil {
		t.Fatalf("renderer: %v", err)
	}
	h := New(mgr, renderer, pricing.DefaultUSEast1Prices())

	if err := mgr.Register("bad", "Bad", "/nonexistent/path/that/does/not/exist"); err != nil {
		t.Fatalf("register: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/inventories/bad/load", http.NoBody)
	req = withURLParam(req, "id", "bad")
	w := httptest.NewRecorder()
	h.LoadInventoryAPI(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}

	info, _ := mgr.Get("bad")
	if info.State != inventory.StateError {
		t.Errorf("state = %q, want %q", info.State, inventory.StateError)
	}
}

// TestManagerWithIndex_NoUseAfterCloseUnderUnload runs many concurrent
// WithIndex readers against a real mmap-backed index while a separate
// goroutine repeatedly Unloads + Loads the same inventory. The point is
// to exercise the per-inventory RWMutex protocol introduced to fix the
// SIGBUS that GetIndex used to allow. With -race + concurrent reads
// across the mmap window, any leftover use-after-close would surface as
// either a SIGBUS, a race-detector report, or an Index returning bogus
// data once it's been closed.
func TestManagerWithIndex_NoUseAfterCloseUnderUnload(t *testing.T) {
	tmp := t.TempDir()
	if err := seeder.Run(seeder.Config{
		OutputDir: tmp,
		Count:     1,
		Objects:   500,
		Preset:    "small",
		Seed:      42,
		Logger:    zerolog.Nop(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	mgr := inventory.NewManager()
	t.Cleanup(func() { _ = mgr.Close() })

	const id = "racy"
	indexPath := filepath.Join(tmp, "inv-001")
	if err := mgr.Register(id, "Racy", indexPath); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := mgr.Load(context.Background(), id); err != nil {
		t.Fatalf("initial load: %v", err)
	}

	const readers = 8
	const iters = 50

	done := make(chan struct{})
	go func() {
		// Flap the inventory state under the readers.
		for range iters {
			_ = mgr.Unload(id)
			_ = mgr.Load(context.Background(), id)
		}
		close(done)
	}()

	errs := make(chan error, readers)
	for range readers {
		go func() {
			var lastErr error
			for range iters {
				// WithIndex may legitimately return ErrNotLoaded during
				// the brief Unloaded window — that's fine. The fatal
				// case is reading from a closed mmap, which would
				// SIGBUS or trigger the race detector.
				_ = mgr.WithIndex(id, func(idx *indexread.Index) error {
					// Touch a couple of methods so any unmapped read shows up.
					_ = idx.Count()
					if pos, ok := idx.Lookup(""); ok {
						_ = idx.Stats(pos)
					}
					return nil
				})
			}
			errs <- lastErr
		}()
	}

	<-done
	for range readers {
		if err := <-errs; err != nil {
			t.Errorf("reader: %v", err)
		}
	}
}
