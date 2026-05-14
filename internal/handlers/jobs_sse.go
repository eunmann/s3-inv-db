package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

// JobsStream serves text/event-stream and emits one event per job state
// change. Browsers can subscribe with the htmx-sse extension; event
// name = job ID so a single subscription can drive any number of row
// swaps. Data is a JSON snapshot — clients are free to render however
// they like.
func (h *Handlers) JobsStream(w http.ResponseWriter, r *http.Request) {
	if h.jobBus == nil {
		http.Error(w, "jobs not configured", http.StatusServiceUnavailable)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	// Disable proxy buffering (nginx etc.).
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	events, cancel := h.jobBus.Subscribe()
	defer cancel()

	logger := zerolog.Ctx(r.Context())
	ctx := r.Context()
	// Emit an initial comment to nail the connection open.
	if _, err := fmt.Fprintf(w, ": connected\n\n"); err != nil {
		return
	}
	flusher.Flush()

	// Periodic keep-alive comment surfaces a dead client (browser
	// navigated away, idle TCP socket Chrome will close in ~60s) via a
	// write error well before the per-origin connection limit fills up.
	heartbeat := time.NewTicker(h.sseHeartbeat)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if _, err := fmt.Fprintf(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case j, ok := <-events:
			if !ok {
				return
			}
			payload, err := json.Marshal(j)
			if err != nil {
				logger.Error().Err(err).Msg("marshal job event")
				continue
			}
			// Two frames per change so different listeners can subscribe
			// by job ID (debug consoles) or by inventory ID (the row
			// elements, which don't know which job is theirs).
			if _, err := fmt.Fprintf(w, "event: job-%s\ndata: %s\n\n", j.ID, payload); err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "event: row-%s\ndata: %s\n\n", j.InventoryID, payload); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
