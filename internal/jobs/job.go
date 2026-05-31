// Package jobs runs inventory-load and inventory-build work
// asynchronously. A Scheduler owns the in-memory registry of live jobs +
// their cancel handles, a Store persists the latest snapshot of each
// job for cross-restart visibility, and a Bus fans out state changes
// to subscribers (the SSE handler today).
package jobs

import (
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// ID is the typed identifier of a job row. Distinct from inventory.ID
// so the compiler catches swaps when both flow through one call.
type ID string

func (id ID) String() string { return string(id) }

// Kind classifies what a job is doing.
type Kind string

const (
	KindBuild  Kind = "build"
	KindUnload Kind = "unload"
)

// State is the job lifecycle. Happy path: queued → running → succeeded.
// Failed/cancelled are alternate terminals; aborted is set at server
// startup for jobs left running from the previous process (mid-pipeline
// resume is unsafe).
type State string

const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
	StateAborted   State = "aborted"
)

func (s State) IsTerminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateCancelled, StateAborted:
		return true
	case StateQueued, StateRunning:
		return false
	}

	return false
}

func (s State) IsLive() bool {
	return s == StateQueued || s == StateRunning
}

// Job is one unit of background work.
//
// StageDone / StageTotal hold the current stage's step count and step
// total (e.g. chunks-parsed / chunks-total during downloading, merged
// run-files / total run-files during building). The unit varies by
// stage, so callers should treat them as opaque progress numbers, not
// bytes. They are persisted in legacy SQL columns bytes_done /
// bytes_total — those names predate the units becoming polymorphic
// and are kept to avoid a schema migration.
type Job struct {
	StartedAt    time.Time
	FinishedAt   time.Time
	UpdatedAt    time.Time
	ID           ID
	InventoryID  inventory.ID
	Kind         Kind
	State        State
	Stage        string
	Error        string
	PrevJobID    ID
	Stages       []StageRecord
	Progress     int
	AttemptCount int
	StageTotal   int64
	StageDone    int64
}

// StageRecord is one entry in the per-job pipeline timeline. EndedAt
// zero means the stage is still in flight.
type StageRecord struct {
	StartedAt time.Time     `json:"started_at,omitzero"`
	EndedAt   time.Time     `json:"ended_at,omitzero"`
	Name      string        `json:"name"`
	Err       string        `json:"err,omitempty"`
	Duration  time.Duration `json:"duration_ns,omitempty"`
	Rows      uint64        `json:"rows,omitempty"`
	Bytes     uint64        `json:"bytes,omitempty"`
}

// InProgress is true while the stage's EndedAt hasn't been stamped.
func (r StageRecord) InProgress() bool { return r.EndedAt.IsZero() }

// Update is the diff a Work function reports back during execution. Zero
// fields are ignored so callers can update one dimension at a time.
// StageDone/StageTotal carry the current stage's step counter — units
// are stage-dependent (chunks, run files, …), not bytes.
type Update struct {
	Stage      string
	Stages     []StageRecord
	Progress   int
	StageTotal int64
	StageDone  int64
}
