package handlers

import (
	"net/http"
	"sort"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// Notification is one surfaced failure or warning.
type Notification struct {
	Kind    string    `json:"kind"` // "poll" | "load" | "budget"
	Title   string    `json:"title"`
	Message string    `json:"message"`
	Target  string    `json:"target,omitempty"` // config id or run id
	At      time.Time `json:"at"`
}

// NotificationsResponse is the JSON the banner consumes.
type NotificationsResponse struct {
	Notifications []Notification `json:"notifications"`
}

func (h *Handlers) collectNotifications() []Notification {
	out := make([]Notification, 0, 8)
	if h.configStore != nil {
		configs, err := h.configStore.List()
		if err == nil {
			for i := range configs {
				c := &configs[i]
				if c.LastPollError == "" {
					continue
				}
				out = append(out, Notification{
					Kind:    "poll",
					Title:   "Discovery poll failed",
					Message: c.LastPollError,
					Target:  c.Source + "/" + c.Name,
					At:      c.LastPolledAt,
				})
			}
		}
	}
	infos := h.manager.List()
	for i := range infos {
		info := &infos[i]
		if info.AutoLoadFailureCount == 0 || info.Error == "" {
			continue
		}
		title := "Auto-load failed"
		if info.AutoLoadBackoffUntil.After(time.Now()) {
			title += " (backing off)"
		}
		out = append(out, Notification{
			Kind:    "load",
			Title:   title,
			Message: info.Error,
			Target:  string(info.ID),
			At:      info.AutoLoadBackoffUntil,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].At.After(out[j].At) })
	return out
}

// NotificationsAPI returns aggregated failure notifications for the
// page-level banner.
func (h *Handlers) NotificationsAPI(w http.ResponseWriter, _ *http.Request) {
	WriteJSON(w, http.StatusOK, NotificationsResponse{Notifications: h.collectNotifications()})
}

// NotificationsPartial renders the banner HTML; empty when nothing to surface.
func (h *Handlers) NotificationsPartial(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	notifs := h.collectNotifications()
	if len(notifs) == 0 {
		return
	}
	data := struct {
		Notifications []Notification
	}{Notifications: notifs}
	if err := h.renderer.RenderPartial(w, "notifications_banner.html", data); err != nil {
		http.Error(w, "render notifications", http.StatusInternalServerError)
	}
}

var _ = inventory.StateLoaded // keep inventory imported for future fields
