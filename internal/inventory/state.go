// Package inventory owns the inventory entity, its typed identifier,
// and the in-memory Manager + SQLite Store that coordinate state
// across discovery, loading, and serving. Discovery primitives
// (walking S3, parsing manifests) live in the s3disco package, which
// imports this one to construct Inventory values — the dependency
// runs s3disco → inventory only.
package inventory

import (
	"strings"
	"time"
)

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

// IsLoaded + siblings let templates avoid stringly-typed comparisons
// so a state rename becomes a compiler-caught refactor.
func (s State) IsLoaded() bool    { return s == StateLoaded }
func (s State) IsNotLoaded() bool { return s == StateNotLoaded }
func (s State) IsLoading() bool   { return s == StateLoading }
func (s State) IsError() bool     { return s == StateError }

// CanLoad: Not-loaded and Error inventories can both be (re)built.
func (s State) CanLoad() bool {
	return s == StateNotLoaded || s == StateError
}

// ID is the typed identifier for one inventory run, formatted
// "<source-bucket>/<inventory-name>/<run-timestamp>". The named type
// distinguishes inventory IDs from jobs.ID at function signatures and
// carries the splitting/grouping logic that handlers would otherwise
// duplicate. Persisted as TEXT in the SQLite inventory table.
type ID string

// String makes ID print transparently in logs and format strings.
func (id ID) String() string { return string(id) }

// IDParts is the result of (ID).Split: the three slash-separated
// segments of a composite ID plus an OK flag.
type IDParts struct {
	Source    string
	Inventory string
	Run       string
	OK        bool
}

// Split returns the three segments of a 3-part inventory ID. The
// OK field is false for any input that doesn't split into exactly
// three slash-separated parts (placeholder configurations with no
// completed run, legacy 2-part entries, or hand-registered
// inventories).
func (id ID) Split() IDParts {
	parts := strings.SplitN(string(id), "/", 3)
	if len(parts) != 3 {
		return IDParts{}
	}

	return IDParts{Source: parts[0], Inventory: parts[1], Run: parts[2], OK: true}
}

// ConfigID returns "<source-bucket>/<inventory-name>" — the identifier
// shared across every run of one inventory configuration. Returns the
// whole string for non-3-part IDs so callers always have a non-empty
// grouping key.
func (id ID) ConfigID() string {
	if p := id.Split(); p.OK {
		return p.Source + "/" + p.Inventory
	}

	return string(id)
}

// Inventory is the value type describing one discovered S3 Inventory
// run. The discovery layer mints these from S3 listings; the Manager
// tracks each one's lifecycle state via Info.
type Inventory struct {
	SourceBucket      string `json:"source_bucket"`
	Name              string `json:"inventory_name"`
	Run               string `json:"run"`
	ManifestKey       string `json:"manifest_key"`
	FileFormat        string `json:"file_format,omitempty"`
	CreationTimestamp string `json:"creation_timestamp,omitempty"`
	Error             string `json:"error,omitempty"`
	FileCount         int    `json:"file_count,omitempty"`
	TotalBytes        int64  `json:"total_bytes,omitempty"`
}

// CompositeID returns the typed identifier the Manager uses as its
// primary key. When Run is empty (no completed runs yet) only
// "<src>/<inv>" is returned — the placeholder is not independently
// loadable.
func (i Inventory) CompositeID() ID {
	if i.Run == "" {
		return ID(i.SourceBucket + "/" + i.Name)
	}

	return ID(i.SourceBucket + "/" + i.Name + "/" + i.Run)
}

// ConfigID returns "<source-bucket>/<inventory-name>" — the identifier
// shared across every run of one inventory configuration.
func (i Inventory) ConfigID() string {
	return i.SourceBucket + "/" + i.Name
}

// Info contains metadata about a managed inventory.
type Info struct {
	LoadedAt             time.Time `json:"loaded_at,omitzero"`
	LastAccessedAt       time.Time `json:"last_accessed_at,omitzero"`
	AutoLoadBackoffUntil time.Time `json:"auto_load_backoff_until,omitzero"`
	UserUnloadedAt       time.Time `json:"user_unloaded_at,omitzero"`
	Name                 string    `json:"name"`
	Path                 string    `json:"path"`
	State                State     `json:"state"`
	Error                string    `json:"error,omitempty"`
	ID                   ID        `json:"id"`
	IndexBytes           uint64    `json:"index_bytes,omitempty"`
	NodeCount            uint64    `json:"node_count,omitempty"`
	MaxDepth             uint32    `json:"max_depth,omitempty"`
	AutoLoadFailureCount uint32    `json:"auto_load_failure_count,omitempty"`
	// LoadDuration is the wall-clock time of the most recent successful
	// load (StateLoading → StateLoaded transition). Persisted to SQLite
	// so a server restart preserves it for runs that are still in
	// StateLoaded. Surfaced to the UI so auto-loaded runs (which do not
	// go through jobs.Manager and therefore have no JobStore record)
	// still show a load time.
	LoadDuration time.Duration `json:"load_duration_ns,omitempty"`
	Pinned       bool          `json:"pinned"`
	HasTierData  bool          `json:"has_tier_data"`
}
