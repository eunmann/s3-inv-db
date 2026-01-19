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
