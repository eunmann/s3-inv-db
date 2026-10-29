package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
)

// clientIPFromRequest returns the client IP the server's client-IP
// middleware resolved, falling back to RemoteAddr without its port when
// the middleware isn't installed (handler tests). Malformed inputs
// collapse to the literal RemoteAddr so we never key on "".
func clientIPFromRequest(r *http.Request) string {
	if ip := middleware.GetClientIP(r.Context()); ip != "" {
		return ip
	}
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
	ID            string       `json:"ID"`
	InventoryID   string       `json:"InventoryID"`
	Kind          string       `json:"Kind"`
	State         string       `json:"State"`
	Stage         string       `json:"Stage"`
	StageLabel    string       `json:"StageLabel,omitempty"`
	Step          string       `json:"Step,omitempty"`
	StepStartedAt string       `json:"StepStartedAt,omitempty"`
	StartedAt     string       `json:"StartedAt,omitempty"`
	FinishedAt    string       `json:"FinishedAt,omitempty"`
	Error         string       `json:"Error,omitempty"`
	UpdatedAt     string       `json:"UpdatedAt,omitempty"`
	PrevJobID     string       `json:"PrevJobID,omitempty"`
	Stages        []stageEvent `json:"Stages,omitempty"`
	Progress      int          `json:"Progress"`
	AttemptCount  int          `json:"AttemptCount,omitempty"`
	StageTotal    int64        `json:"StageTotal"`
	StageDone     int64        `json:"StageDone"`
	SpillCount    int          `json:"SpillCount,omitempty"`
	SpillBytes    int64        `json:"SpillBytes,omitempty"`
	MergeRounds   int          `json:"MergeRounds,omitempty"`
	MergeBytes    int64        `json:"MergeBytes,omitempty"`
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
		StageLabel:   templates.StageLabel(j.Stage),
		Step:         j.Step,
		Progress:     j.Progress,
		StageTotal:   j.StageTotal,
		StageDone:    j.StageDone,
		Error:        j.Error,
		AttemptCount: j.AttemptCount,
		PrevJobID:    string(j.PrevJobID),
		SpillCount:   j.SpillCount,
		SpillBytes:   j.SpillBytes,
		MergeRounds:  j.MergeRounds,
		MergeBytes:   j.MergeBytes,
	}
	if !j.StartedAt.IsZero() {
		ev.StartedAt = j.StartedAt.Format(time.RFC3339Nano)
	}
	if !j.FinishedAt.IsZero() {
		ev.FinishedAt = j.FinishedAt.Format(time.RFC3339Nano)
	}
	if !j.StepStartedAt.IsZero() {
		ev.StepStartedAt = j.StepStartedAt.Format(time.RFC3339Nano)
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

// sseRetryMs is the reconnect delay the stream asks browsers to use
// after the connection drops (server restart, overflow, network blip).
const sseRetryMs = 2000

// inventoryEvent is the payload of an "inventory" frame: the catalog
// state of InventoryID changed outside a job (loaded from cache,
// evicted, unloaded, pinned), so clients should re-read it.
type inventoryEvent struct {
	InventoryID string `json:"InventoryID"`
}

// JobsStream serves text/event-stream. On connect it sends a "job"
// frame for every job whose run is still in progress (queued, running,
// or terminal while that final state is being written), then a
// "synced" frame; after that a "job" frame (the job's JSON snapshot)
// whenever a job changes and an "inventory" frame whenever an
// inventory's catalog state changes outside a job. Frames are coalesced per job and paced to at
// most one batch per ssePace, so a busy build costs a few frames a
// second, never one per progress tick, and a job's terminal state is
// always delivered. A reader that falls too far behind is disconnected:
// the browser reconnects and resynchronises from the connect snapshot.
func (h *Handlers) JobsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)

		return
	}

	// Per-IP cap: each subscriber holds a queue inside the jobs.Bus;
	// unbounded subscriptions per client are an OOM vector.
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

	// Subscribe before taking the snapshot so no change falls between
	// the two; a change seen twice is harmless.
	sub := h.jobBus.Subscribe()
	defer sub.Close()

	logger := zerolog.Ctx(r.Context())
	ctx := r.Context()
	if _, err := fmt.Fprintf(w, "retry: %d\n: connected\n\n", sseRetryMs); err != nil {
		return
	}
	live := h.jobMgr.Live()
	for i := range live {
		if err := writeJobFrame(w, live[i]); err != nil {
			logger.Debug().Err(err).Msg("write job snapshot")

			return
		}
	}
	if _, err := fmt.Fprint(w, "event: synced\ndata: {}\n\n"); err != nil {
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
		case <-sub.Ready():
			evs, overflowed := sub.Drain()
			if overflowed {
				// Too far behind to know what was missed: end the
				// stream; the browser reconnects after sseRetryMs and
				// starts from a fresh snapshot.
				return
			}
			if err := writeEvents(w, evs); err != nil {
				logger.Debug().Err(err).Msg("write job events")

				return
			}
			flusher.Flush()
			// Pace: changes during the pause coalesce into the next batch.
			select {
			case <-ctx.Done():
				return
			case <-time.After(ssePace):
			}
		}
	}
}

func writeEvents(w io.Writer, evs []jobs.Event) error {
	for _, ev := range evs {
		if ev.Job != nil {
			if err := writeJobFrame(w, *ev.Job); err != nil {
				return err
			}

			continue
		}
		payload, err := json.Marshal(inventoryEvent{InventoryID: string(ev.Inventory)})
		if err != nil {
			return fmt.Errorf("marshal inventory event: %w", err)
		}
		if _, err := fmt.Fprintf(w, "event: inventory\ndata: %s\n\n", payload); err != nil {
			return fmt.Errorf("write inventory event: %w", err)
		}
	}

	return nil
}

func writeJobFrame(w io.Writer, j jobs.Job) error {
	payload, err := json.Marshal(jobToEvent(j))
	if err != nil {
		return fmt.Errorf("marshal job event: %w", err)
	}
	if _, err := fmt.Fprintf(w, "event: job\ndata: %s\n\n", payload); err != nil {
		return fmt.Errorf("write job event: %w", err)
	}

	return nil
}
