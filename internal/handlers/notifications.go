package handlers

import (
	"context"
	"net/http"
	"slices"
	"time"
)

// Notification is one surfaced failure or warning.
type Notification struct {
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Title   string    `json:"title"`
	Message string    `json:"message"`
	Target  string    `json:"target,omitempty"`
}

// NotificationsResponse is the JSON the banner consumes.
type NotificationsResponse struct {
	Notifications []Notification `json:"notifications"`
}

func (h *Handlers) collectNotifications(ctx context.Context) []Notification {
	out := make([]Notification, 0, 8)
	configs, err := h.configStore.List(ctx)
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
			At:      info.LastAutoLoadFailedAt,
		})
	}
	slices.SortFunc(out, func(a, b Notification) int { return b.At.Compare(a.At) })

	return out
}

// NotificationsAPI returns aggregated failure notifications for the
// page-level banner.
func (h *Handlers) NotificationsAPI(w http.ResponseWriter, r *http.Request) {
	WriteJSON(w, http.StatusOK, NotificationsResponse{Notifications: h.collectNotifications(r.Context())})
}

// NotificationsPartial renders the banner HTML; empty when nothing to surface.
func (h *Handlers) NotificationsPartial(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", contentTypeHTML)
	notifs := h.collectNotifications(r.Context())
	if len(notifs) == 0 {
		return
	}
	data := struct {
		Notifications []Notification
	}{Notifications: notifs}
	h.renderHTMLPartial(w, r, "notifications_banner.html", "render notifications", data)
}
