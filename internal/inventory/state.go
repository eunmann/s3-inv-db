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

// ID is the typed identifier for one inventory run, formatted
// "<source-bucket>/<inventory-name>/<run-timestamp>". The named type
// distinguishes inventory IDs from jobs.ID at function signatures and
// carries the splitting/grouping logic that handlers would otherwise
// duplicate. Persisted as TEXT in the SQLite inventory table.
type ID string

// String makes ID print transparently in logs and format strings.
func (id ID) String() string { return string(id) }

// Split returns the three segments of a 3-part inventory ID. The ok
// return is false for any input that doesn't split into exactly three
// slash-separated parts (placeholder configurations with no completed
// run, legacy 2-part entries, or hand-registered inventories).
func (id ID) Split() (sourceBucket, inventoryName, run string, ok bool) {
	parts := strings.SplitN(string(id), "/", 3)
	if len(parts) != 3 {
		return "", "", "", false
	}

	return parts[0], parts[1], parts[2], true
}

// ConfigID returns "<source-bucket>/<inventory-name>" — the identifier
// shared across every run of one inventory configuration. Returns the
// whole string for non-3-part IDs so callers always have a non-empty
// grouping key.
func (id ID) ConfigID() string {
	if src, inv, _, ok := id.Split(); ok {
		return src + "/" + inv
	}

	return string(id)
}

// Inventory is the value type describing one discovered S3 Inventory
// run. The discovery layer mints these from S3 listings; the Manager
// tracks each one's lifecycle state via Info.
type Inventory struct {
	// SourceBucket is the bucket the inventory is *describing* (the
	// segment S3 inserts between the destination prefix and the
	// inventory-name).
	SourceBucket string `json:"source_bucket"`

	// InventoryName is the AWS S3 inventory configuration's Id (its
	// slug). Named "InventoryName" so it doesn't shadow the ID type —
	// the field is one segment of an ID, not the whole thing.
	InventoryName string `json:"inventory_name"`

	// Run is the timestamp folder name (e.g., "2026-05-13T03-02Z"). Empty
	// when the configuration has been discovered but has no completed
	// runs yet — in that case the entry is returned as a placeholder so
	// the UI can surface "no runs yet".
	Run string `json:"run"`

	// ManifestKey is the S3 key of this run's manifest.json. Empty when
	// Run is empty.
	ManifestKey string `json:"manifest_key"`

	// FileFormat reported by the manifest ("CSV", "Parquet").
	FileFormat string `json:"file_format,omitempty"`

	// FileCount is the number of data files referenced by the manifest.
	// A coarse "size" signal for the UI before download.
	FileCount int `json:"file_count,omitempty"`

	// CreationTimestamp is the manifest's reported creation time
	// (UnixMilli as a decimal string, exactly as S3 writes it).
	CreationTimestamp string `json:"creation_timestamp,omitempty"`

	// Error captures a non-fatal per-run failure (e.g. unreadable
	// manifest). Empty on success.
	Error string `json:"error,omitempty"`
}

// CompositeID returns the typed identifier the Manager uses as its
// primary key. When Run is empty (no completed runs yet) only
// "<src>/<inv>" is returned — the placeholder is not independently
// loadable.
func (i Inventory) CompositeID() ID {
	if i.Run == "" {
		return ID(i.SourceBucket + "/" + i.InventoryName)
	}

	return ID(i.SourceBucket + "/" + i.InventoryName + "/" + i.Run)
}

// ConfigID returns "<source-bucket>/<inventory-name>" — the identifier
// shared across every run of one inventory configuration.
func (i Inventory) ConfigID() string {
	return i.SourceBucket + "/" + i.InventoryName
}

// Info contains metadata about a managed inventory.
type Info struct {
	ID          ID        `json:"id"`
	Name        string    `json:"name"`
	Path        string    `json:"path"`
	State       State     `json:"state"`
	Error       string    `json:"error,omitempty"`
	NodeCount   uint64    `json:"node_count,omitempty"`
	MaxDepth    uint32    `json:"max_depth,omitempty"`
	LoadedAt    time.Time `json:"loaded_at,omitempty"`
	HasTierData bool      `json:"has_tier_data"`

	// Pinned runs are never auto-evicted. Manual Load sets this true;
	// manual Unload sets it false.
	Pinned bool `json:"pinned"`

	// UserUnloadedAt is set when the user manually unloads a run. The
	// auto-loader skips runs that have a non-zero UserUnloadedAt so a
	// deliberate unload sticks across poll cycles. Cleared on the next
	// manual Load.
	UserUnloadedAt time.Time `json:"user_unloaded_at,omitempty"`

	// IndexBytes is the on-disk size of the materialised index in bytes,
	// measured after a successful Load. Zero for non-loaded runs.
	IndexBytes uint64 `json:"index_bytes,omitempty"`

	// AutoLoadFailureCount and AutoLoadBackoffUntil track per-run
	// backoff state for the auto-loader; cleared on successful load.
	AutoLoadFailureCount uint32    `json:"auto_load_failure_count,omitempty"`
	AutoLoadBackoffUntil time.Time `json:"auto_load_backoff_until,omitempty"`

	// LastAccessedAt is updated whenever a reader (WithIndex /
	// WithTwoIndexes) touches the index. Drives the LRU tiebreak in
	// eviction planning.
	LastAccessedAt time.Time `json:"last_accessed_at,omitempty"`
}
