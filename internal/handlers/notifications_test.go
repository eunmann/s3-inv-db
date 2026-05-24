package handlers_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

	req := httptest.NewRequest(http.MethodGet, "/api/notifications", http.NoBody)
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
