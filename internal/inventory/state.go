// Package inventory manages multiple S3 inventory indexes with thread-safe access.
package inventory

import "time"

// State represents the lifecycle state of an inventory.
type State string

// Inventory states. String values match the user-facing vocabulary
// the templates render — no separate enum-to-label table to keep in
// sync.
//
// Unload wipes the on-disk cache; there is no "Unloaded but cached
// on disk" intermediate state. After an Unload the row is
// StateNotLoaded again and the next Load builds from S3.
const (
	StateNotLoaded State = "not_loaded" // discovered, no cache on disk
	StateLoading   State = "loading"    // build pipeline running
	StateLoaded    State = "loaded"     // index in memory + on disk
	StateError     State = "error"      // last operation failed
)

// State predicates. Templates use these instead of stringly-typed
// {{eq (printf "%s" .State) "loaded"}} comparisons so a state rename
// becomes a refactor that compiler catches.
func (s State) IsLoaded() bool    { return s == StateLoaded }
func (s State) IsNotLoaded() bool { return s == StateNotLoaded }
func (s State) IsLoading() bool   { return s == StateLoading }
func (s State) IsError() bool     { return s == StateError }

// CanLoad reports whether a Load is a legal next operation. Not-loaded
// and Error inventories can both be (re)built.
func (s State) CanLoad() bool {
	return s == StateNotLoaded || s == StateError
}

// Info contains metadata about a managed inventory.
type Info struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Path        string    `json:"path"`
	State       State     `json:"state"`
	Error       string    `json:"error,omitempty"`
	NodeCount   uint64    `json:"node_count,omitempty"`
	MaxDepth    uint32    `json:"max_depth,omitempty"`
	LoadedAt    time.Time `json:"loaded_at,omitempty"`
	HasTierData bool      `json:"has_tier_data"`
}
