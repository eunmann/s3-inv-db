// Package jobs runs inventory-load and inventory-build work
// asynchronously. A Manager owns the in-memory registry of live jobs +
// their cancel handles, a Store persists the latest snapshot of each
// job for cross-restart visibility, and a Bus fans out state changes
// to subscribers (the SSE handler today).
package jobs

import (
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// ID is the typed identifier of a job row in the SQLite jobs table.
// Distinct from inventory.ID so the compiler catches accidental
// swaps when both flow through the same call (Store.Upsert,
// SSE event names, etc).
type ID string

// String makes ID print transparently in logs and format strings.
func (id ID) String() string { return string(id) }

// Kind classifies what a job is doing.
type Kind string

// Kind values.
const (
	KindBuild  Kind = "build"
	KindUnload Kind = "unload"
)

// State is the lifecycle state of a job.
type State string

// State values. Happy path is queued → running → succeeded. Failed
// and cancelled are alternate terminals; aborted is set at server
// startup for jobs left running from the previous process (we can't
// safely resume mid-pipeline).
const (
	StateQueued    State = "queued"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateCancelled State = "cancelled"
	StateAborted   State = "aborted"
)

// IsTerminal reports whether a state is a final resting state — no
// further transitions will occur from here.
func (s State) IsTerminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateCancelled, StateAborted:
		return true
	}

	return false
}

// IsLive reports whether the job is currently consuming resources or
// queued to do so.
func (s State) IsLive() bool {
	return s == StateQueued || s == StateRunning
}

// Job is one unit of background work.
type Job struct {
	ID          ID
	InventoryID inventory.ID
	Kind        Kind
	State       State
	Stage       string
	Progress    int
	BytesTotal  int64
	BytesDone   int64
	StartedAt   time.Time
	FinishedAt  time.Time
	Error       string
	UpdatedAt   time.Time
}

// Update is the diff a Work function reports back during execution. Zero
// fields are ignored so callers can update one dimension at a time.
type Update struct {
	Stage      string
	Progress   int
	BytesTotal int64
	BytesDone  int64
}
