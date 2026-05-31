package handlers

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/rs/zerolog"
)

// clientIPFromRequest returns the remote IP from RemoteAddr stripped
// of its port. Best-effort: malformed inputs collapse to the literal
// RemoteAddr so we never key on the empty string.
func clientIPFromRequest(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}

// jobEvent is the JSON envelope the SSE stream emits. Defined here
// (rather than reusing jobs.Job directly) so the wire format is owned
// by the HTTP layer — renames inside the jobs package can't accidentally
// change what browsers see, and every field carries an explicit json tag.
type jobEvent struct {
	ID           string       `json:"ID"`
	InventoryID  string       `json:"InventoryID"`
	Kind         string       `json:"Kind"`
	State        string       `json:"State"`
	Stage        string       `json:"Stage"`
	StartedAt    string       `json:"StartedAt,omitempty"`
	FinishedAt   string       `json:"FinishedAt,omitempty"`
	Error        string       `json:"Error,omitempty"`
	UpdatedAt    string       `json:"UpdatedAt,omitempty"`
	PrevJobID    string       `json:"PrevJobID,omitempty"`
	Stages       []stageEvent `json:"Stages,omitempty"`
	Progress     int          `json:"Progress"`
	AttemptCount int          `json:"AttemptCount,omitempty"`
	StageTotal   int64        `json:"StageTotal"`
	StageDone    int64        `json:"StageDone"`
}

// stageEvent is the per-stage timeline entry inside jobEvent.Stages.
// Short JSON field names because Stages ships on every progress update
// and can carry 4–6 entries per frame.
type stageEvent struct {
	Name       string `json:"n"`
	StartedAt  string `json:"s,omitempty"`
	EndedAt    string `json:"e,omitempty"`
	Err        string `json:"err,omitempty"`
	DurationMs int64  `json:"d,omitempty"`
	Bytes      uint64 `json:"b,omitempty"`
	Rows       uint64 `json:"r,omitempty"`
	InProgress bool   `json:"ip,omitempty"`
}

func stageRecordToEvent(s jobs.StageRecord) stageEvent {
	out := stageEvent{
		Name:       s.Name,
		Err:        s.Err,
		Bytes:      s.Bytes,
		Rows:       s.Rows,
		DurationMs: s.Duration.Milliseconds(),
		InProgress: s.InProgress(),
	}
	if !s.StartedAt.IsZero() {
		out.StartedAt = s.StartedAt.Format(time.RFC3339Nano)
	}
	if !s.EndedAt.IsZero() {
		out.EndedAt = s.EndedAt.Format(time.RFC3339Nano)
	}

	return out
}

func jobToEvent(j jobs.Job) jobEvent {
	ev := jobEvent{
		ID:           string(j.ID),
		InventoryID:  string(j.InventoryID),
		Kind:         string(j.Kind),
		State:        string(j.State),
		Stage:        j.Stage,
		Progress:     j.Progress,
		StageTotal:   j.StageTotal,
		StageDone:    j.StageDone,
		Error:        j.Error,
		AttemptCount: j.AttemptCount,
		PrevJobID:    string(j.PrevJobID),
	}
	if !j.StartedAt.IsZero() {
		ev.StartedAt = j.StartedAt.Format(time.RFC3339Nano)
	}
	if !j.FinishedAt.IsZero() {
		ev.FinishedAt = j.FinishedAt.Format(time.RFC3339Nano)
	}
	if !j.UpdatedAt.IsZero() {
		ev.UpdatedAt = j.UpdatedAt.Format(time.RFC3339Nano)
	}
	if len(j.Stages) > 0 {
		ev.Stages = make([]stageEvent, len(j.Stages))
		for i, s := range j.Stages {
			ev.Stages[i] = stageRecordToEvent(s)
		}
	}

	return ev
}

// JobsStream serves text/event-stream and emits one event per job state
// change. Browsers can subscribe with the htmx-sse extension; event
// name = job ID so a single subscription can drive any number of row
// swaps. Data is a JSON snapshot — clients are free to render however
// they like.
func (h *Handlers) JobsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)

		return
	}

	// Per-IP cap: each subscriber holds a buffered channel inside the
	// jobs.Bus; unbounded subscriptions per client are an OOM vector.
	ip := clientIPFromRequest(r)
	count := h.acquireSSESlot(ip)
	defer h.releaseSSESlot(ip)
	if count > int64(sseMaxConnsPerIP) {
		http.Error(w, "too many SSE connections", http.StatusTooManyRequests)

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
	heartbeat := time.NewTicker(sseHeartbeat)
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
			payload, err := json.Marshal(jobToEvent(j))
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
