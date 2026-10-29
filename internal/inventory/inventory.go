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

// IndexFileFormat is the FileFormat of a published index's run: its
// TotalBytes are the index's own size.
const IndexFileFormat = "index"

// Index bytes per byte of an inventory's data files, the disk a load
// reserves before it builds when its configuration has no built run to
// learn from. An index's size follows its prefix count,
// which no manifest tells, so the ratios cover the densest measured
// layout: built through the pipeline from the AWS-like seeded CSV
// fixtures, indexes were 0.98× (one directory per object) to 2.99×
// (deep_pyramid, 20K objects) their gzipped inventory. Parquet data
// files are about 1.3× the gzipped CSV of the same objects. The gate
// settles a load at its measured size once built, so a layout denser
// still is corrected, not over-committed.
const (
	csvIndexRatio     = 3.0
	parquetIndexRatio = 2.3
)

// A configuration's key layout is stable from run to run, so the ratio
// its previous run measured replaces the conservative one, times
// learnedRatioMargin. Built through the pipeline from the seeded
// realistic, deep_pyramid and unique_dirs inventories at 50K–120K
// objects, a layout's ratio rose by at most 5.5% as it grew, and 14%
// when the same keys came sorted rather than shuffled (gzip packs
// sorted keys tighter; S3 promises no order); together 1.20×, inside
// the margin. Converted across formats it fell up to 9% short, Parquet
// files being 1.28–1.43× the gzipped CSV rather than the 1.30 the
// conservative ratios assume, so a format switch on top of both needs
// about 1.32×: the headroom absorbs a small miss and the gate settles a
// larger one at the built size.
//
// A learned estimate's ratio, margin included, is held to at least
// minLearnedShare of the format's conservative one (0.1 for CSV), so a
// corrupt stored value doesn't reserve nothing, and to at most the
// conservative ratio itself: a run too small for its index's fixed
// overhead measures a ratio above any layout's, and a refused load
// records no new one, so a higher ceiling would keep refusing that
// configuration's later runs. Below the floor are near-flat buckets,
// whose few prefixes make any reservation cheap; above the ceiling the
// gate settles a denser layout as it does with no history.
const (
	learnedRatioMargin = 1.25
	minLearnedShare    = 1.0 / 30
)

// IndexRatio is the index bytes per byte of inventory data files that a
// configuration's latest built run measured, and the file format of
// that run. The zero value is no history.
type IndexRatio struct {
	FileFormat string
	Ratio      float64
}

// EstimateIndexBytes returns the disk loading a run takes, from its
// manifest's file format and data-file total: exact for a published
// index; else the configuration's learned ratio, converted to this
// run's format, plus the margin, at most the conservative ratio; else,
// with no history, the format's conservative ratio. An unknown format counts as CSV, the larger.
func EstimateIndexBytes(fileFormat string, dataBytes uint64, learned IndexRatio) uint64 {
	if fileFormat == IndexFileFormat {
		return dataBytes
	}
	conservative := conservativeIndexRatio(fileFormat)
	ratio := conservative
	if learned.FileFormat != "" {
		ratio = learned.Ratio * learnedRatioMargin
		if from := conservativeIndexRatio(learned.FileFormat); from != conservative {
			ratio *= conservative / from
		}
		// Negated so a NaN takes the floor.
		if !(ratio >= conservative*minLearnedShare) {
			ratio = conservative * minLearnedShare
		}
		ratio = min(ratio, conservative)
	}

	return uint64(float64(dataBytes) * ratio)
}

// conservativeIndexRatio is the ratio for a run of fileFormat with no
// history.
func conservativeIndexRatio(fileFormat string) float64 {
	if strings.EqualFold(fileFormat, "Parquet") {
		return parquetIndexRatio
	}

	return csvIndexRatio
}

// Info contains metadata about a managed inventory.
type Info struct {
	LoadedAt             time.Time `json:"loaded_at,omitzero"`
	LastAccessedAt       time.Time `json:"last_accessed_at,omitzero"`
	AutoLoadBackoffUntil time.Time `json:"auto_load_backoff_until,omitzero"`
	LastAutoLoadFailedAt time.Time `json:"last_auto_load_failed_at,omitzero"`
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
	// go through jobs.Scheduler and therefore have no JobStore record)
	// still show a load time.
	LoadDuration time.Duration `json:"load_duration_ns,omitempty"`
	Pinned       bool          `json:"pinned"`
	HasTierData  bool          `json:"has_tier_data"`
}
