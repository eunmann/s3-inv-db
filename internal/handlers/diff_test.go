package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

func TestDiffPage_FirstVisitShowsPicker(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/diff", http.NoBody)
	w := httptest.NewRecorder()
	f.h.DiffPage(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	for _, want := range []string{"Compare runs", `id="from"`, `id="to"`} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q", want)
		}
	}
	// No comparison should render when from/to are absent.
	if strings.Contains(body, "Show unchanged") {
		t.Error("level table rendered without from/to selected")
	}
}

func TestDiffPage_MismatchedConfigsExplains(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/diff?from=bucket-a/inv-1/run1&to=bucket-b/inv-1/run1", http.NoBody)
	w := httptest.NewRecorder()
	f.h.DiffPage(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "same inventory configuration") {
		t.Errorf("body missing same-config error: %s", body)
	}
}

func TestDiffPage_PartialMismatchedConfigsReturns400(t *testing.T) {
	f := newTestFixture(t)
	req := httptest.NewRequest(http.MethodGet, "/diff?from=a/b/c&to=x/y/z", http.NoBody)
	req.Header.Set("HX-Request", "true")
	w := httptest.NewRecorder()
	f.h.DiffPage(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("htmx partial mismatched configs status = %d, want 400", w.Code)
	}
}

func TestDiffPage_PartialBothLoadedRequired(t *testing.T) {
	f := newTestFixture(t)
	// Register two non-loaded entries with the same configuration.
	if err := f.mgr.Register("bucket-a/inv-1/run1", "a r1", "/p1"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := f.mgr.Register("bucket-a/inv-1/run2", "a r2", "/p2"); err != nil {
		t.Fatalf("register: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/diff?from=bucket-a/inv-1/run1&to=bucket-a/inv-1/run2", http.NoBody)
	req.Header.Set("HX-Request", "true")
	w := httptest.NewRecorder()
	f.h.DiffPage(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("not-loaded partial status = %d, want 409", w.Code)
	}
}

func TestBuildDiffPicker_OnlyLoadedAndThreePart(t *testing.T) {
	in := []inventory.Info{
		{ID: "src-a/inv-1/2026-05-13T03-00Z", State: inventory.StateLoaded},
		{ID: "src-a/inv-1/2026-05-12T03-00Z", State: inventory.StateLoaded},
		{ID: "src-a/inv-2/2026-05-13T03-00Z", State: inventory.StateLoaded},
		{ID: "src-b/inv-1/2026-05-13T03-00Z", State: inventory.StateNotLoaded}, // filtered
		{ID: "legacy-two-part", State: inventory.StateLoaded},                  // filtered (not 3-part)
	}
	got := buildDiffPicker(in)
	if len(got.Groups) != 2 {
		t.Fatalf("groups = %d, want 2 (src-a/inv-1 and src-a/inv-2)", len(got.Groups))
	}
	if got.Groups[0].ConfigLabel != "src-a/inv-1" || got.Groups[1].ConfigLabel != "src-a/inv-2" {
		t.Errorf("group labels = %q, %q; want src-a/inv-1, src-a/inv-2", got.Groups[0].ConfigLabel, got.Groups[1].ConfigLabel)
	}
	if len(got.Groups[0].Options) != 2 {
		t.Errorf("src-a/inv-1 options = %d, want 2 (both loaded runs)", len(got.Groups[0].Options))
	}
	// Newest-first: 2026-05-13 before 2026-05-12.
	if got.Groups[0].Options[0].ID != "src-a/inv-1/2026-05-13T03-00Z" {
		t.Errorf("newest run should be first: got %q", got.Groups[0].Options[0].ID)
	}
}

func TestSameConfig(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"bucket/inv/r1", "bucket/inv/r2", true},
		{"bucket/inv/r1", "bucket/other/r1", false},
		{"buckA/inv/r1", "buckB/inv/r1", false},
		{"two/part", "two/part/three", false},
		{"a/b/c", "a/b", false},
		{"", "", false},
	}
	for _, tc := range cases {
		if got := sameConfig(tc.a, tc.b); got != tc.want {
			t.Errorf("sameConfig(%q,%q) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}
