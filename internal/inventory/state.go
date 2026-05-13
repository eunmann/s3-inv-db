// Package inventory manages multiple S3 inventory indexes with thread-safe access.
package inventory

import "time"

// State represents the lifecycle state of an inventory.
type State string

// Inventory states.
const (
	StatePending  State = "pending"
	StateParsing  State = "parsing"
	StateLoaded   State = "loaded"
	StateError    State = "error"
	StateUnloaded State = "unloaded"
)

// State predicates. Templates use these instead of stringly-typed
// {{eq (printf "%s" .State) "loaded"}} comparisons so a state rename
// becomes a refactor that compiler catches.
func (s State) IsLoaded() bool   { return s == StateLoaded }
func (s State) IsPending() bool  { return s == StatePending }
func (s State) IsParsing() bool  { return s == StateParsing }
func (s State) IsError() bool    { return s == StateError }
func (s State) IsUnloaded() bool { return s == StateUnloaded }

// CanLoad reports whether a Load is a legal next operation. Pending,
// Unloaded, and Error inventories can all be (re)loaded.
func (s State) CanLoad() bool {
	return s == StatePending || s == StateUnloaded || s == StateError
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
