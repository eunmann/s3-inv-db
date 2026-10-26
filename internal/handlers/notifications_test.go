package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// TestNotifications_OrderedByFailureTimeNotFutureBackoff guards the
// notification banner contract: items must be sorted newest-first by
// when the failure occurred, never by the future retry-after time. A
// long-backoff entry must not float above a recent poll failure just
// because its retry-after is further in the future.
func TestNotifications_OrderedByFailureTimeNotFutureBackoff(t *testing.T) {
	h := newTestHandlers(t)
	ctx := t.Context()

	const invID inventory.ID = "src1/inv1/run1"
	if err := h.ManagerForTest().Register(ctx, invID, "inv1", "/path"); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Auto-load failed an hour ago, backoff stretches an hour into the
	// future. The future backoff time must not influence sort order.
	loadFailedAt := time.Now().Add(-1 * time.Hour)
	backoffUntil := time.Now().Add(1 * time.Hour)
	if err := h.ManagerForTest().RecordAutoLoadFailure(ctx, invID, "boom", loadFailedAt, backoffUntil); err != nil {
		t.Fatalf("RecordAutoLoadFailure: %v", err)
	}

	// Poll failure happened one minute ago — much more recent than the
	// auto-load failure.
	pollAt := time.Now().Add(-1 * time.Minute)
	if err := h.ConfigStoreForTest().Upsert(ctx, inventory.Config{
		Source:        "src1",
		Name:          "inv1",
		AutoLoad:      true,
		LastPolledAt:  pollAt,
		LastPollError: "poll boom",
	}); err != nil {
		t.Fatalf("config upsert: %v", err)
	}

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/notifications", http.NoBody)
	w := httptest.NewRecorder()
	h.NotificationsAPI(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp handlers.NotificationsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Notifications) != 2 {
		t.Fatalf("got %d notifications, want 2: %+v", len(resp.Notifications), resp.Notifications)
	}
	// Poll failure happened more recently → must appear first.
	if resp.Notifications[0].Kind != "poll" {
		t.Errorf("first notification kind = %q, want poll (more recent failure)", resp.Notifications[0].Kind)
	}
	if resp.Notifications[1].Kind != "load" {
		t.Errorf("second notification kind = %q, want load (older failure)", resp.Notifications[1].Kind)
	}
}

// TestNotificationsPartial_SummarizesFailures: the banner reads an
// auto-load failure as one sentence and keeps the raw error folded
// under Technical details; the API adds the summary beside the raw
// message.
func TestNotificationsPartial_SummarizesFailures(t *testing.T) {
	h := newTestHandlers(t)
	const invID inventory.ID = "src/inv/2026-10-24T03-00Z"
	if err := h.ManagerForTest().Register(t.Context(), invID, "inv", "/path"); err != nil {
		t.Fatal(err)
	}
	if err := h.ManagerForTest().RecordAutoLoadFailure(t.Context(), invID, missingChunkErr, time.Now(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.NotificationsPartial(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/partials/notifications", http.NoBody))
	doc := parseHTMLFragment(t, w.Body.String())
	item := doc.Find("li")
	if !strings.Contains(item.Children().Eq(1).Text(), "An inventory data file is missing from S3") {
		t.Errorf("banner item doesn't lead with the summary: %q", item.Text())
	}
	tech := item.Find("details")
	if !strings.Contains(tech.Find("summary").Text(), "Technical details") || !strings.Contains(tech.Text(), "RequestID") {
		t.Error("raw error isn't folded under Technical details")
	}

	aw := httptest.NewRecorder()
	h.NotificationsAPI(aw, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/notifications", http.NoBody))
	var resp handlers.NotificationsResponse
	if err := json.NewDecoder(aw.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Notifications) != 1 || resp.Notifications[0].Message != missingChunkErr || !strings.HasPrefix(resp.Notifications[0].Summary, "An inventory data file") {
		t.Errorf("API notification %+v", resp.Notifications)
	}
}
