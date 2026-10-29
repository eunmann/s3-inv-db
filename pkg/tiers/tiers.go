// Package tiers defines S3 storage class and Intelligent-Tiering tier mappings.
package tiers

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/eunmann/s3-inv-db/pkg/logging"
)

// ErrUnknownTierID is returned when a caller looks up a tier id that
// is outside the valid range.
var ErrUnknownTierID = errors.New("unknown tier id")

// ID represents a logical tier identifier.
type ID uint8

// Tier IDs for all S3 storage classes and Intelligent-Tiering access tiers.
const (
	Standard ID = iota
	StandardIA
	OneZoneIA
	GlacierIR
	GlacierFR
	DeepArchive
	ReducedRedundancy
	ITFrequent
	ITInfrequent
	ITArchiveInstant
	ITArchive
	ITDeepArchive
	ITFrequentSmall // IT objects < 128 KiB: Frequent rate, no monitoring fee
	NumTiers        // Sentinel value for array sizing
)

// SmallObjectThresholdBytes is the AWS S3 Intelligent-Tiering minimum
// monitored object size. Objects below this are stored at the Frequent
// Access rate but are not monitored and cannot transition to lower tiers.
// Source: https://aws.amazon.com/s3/storage-classes/intelligent-tiering/
const SmallObjectThresholdBytes uint64 = 128 * 1024

// deepArchive is both an S3 storage class and an Intelligent-Tiering
// access tier.
const deepArchive = "DEEP_ARCHIVE"

// Info describes a storage tier.
type Info struct {
	Name       string `json:"name"`
	FilePrefix string `json:"file"`
	ID         ID     `json:"id"`
}

// allTiers is the canonical tier-ID-ordered list. Kept private so
// callers can only see it via AllTiers, which returns a defensive
// copy — preserves immutability without the per-call slice alloc that
// the previous "return literal" form caused.
//
//nolint:gochecknoglobals // immutable tier table by design
var allTiers = []Info{
	{ID: Standard, Name: "STANDARD", FilePrefix: "standard"},
	{ID: StandardIA, Name: "STANDARD_IA", FilePrefix: "standard_ia"},
	{ID: OneZoneIA, Name: "ONEZONE_IA", FilePrefix: "onezone_ia"},
	{ID: GlacierIR, Name: "GLACIER_IR", FilePrefix: "glacier_ir"},
	{ID: GlacierFR, Name: "GLACIER", FilePrefix: "glacier_fr"},
	{ID: DeepArchive, Name: deepArchive, FilePrefix: "deep_archive"},
	{ID: ReducedRedundancy, Name: "REDUCED_REDUNDANCY", FilePrefix: "reduced_redundancy"},
	{ID: ITFrequent, Name: "INTELLIGENT_TIERING_FREQUENT", FilePrefix: "it_frequent"},
	{ID: ITInfrequent, Name: "INTELLIGENT_TIERING_INFREQUENT", FilePrefix: "it_infrequent"},
	{ID: ITArchiveInstant, Name: "INTELLIGENT_TIERING_ARCHIVE_INSTANT", FilePrefix: "it_archive_instant"},
	{ID: ITArchive, Name: "INTELLIGENT_TIERING_ARCHIVE", FilePrefix: "it_archive"},
	{ID: ITDeepArchive, Name: "INTELLIGENT_TIERING_DEEP_ARCHIVE", FilePrefix: "it_deep_archive"},
	{ID: ITFrequentSmall, Name: "INTELLIGENT_TIERING_FREQUENT_SMALL", FilePrefix: "it_frequent_small"},
}

// AllTiers returns information about all supported tiers in tier-ID
// order. Returns a defensive copy so callers can't mutate the
// canonical table.
func AllTiers() []Info {
	return slices.Clone(allTiers)
}

// AllTierIDs returns every tier ID in ascending order. Used as the
// dense default when a builder records all tiers rather than a known
// present subset.
func AllTierIDs() []ID {
	ids := make([]ID, NumTiers)
	for i := range ids {
		ids[i] = ID(i)
	}

	return ids
}

// Mapping provides tier lookup and metadata.
type Mapping struct {
	indexByS3Name map[string]ID
	// warned records unknown storage classes / access tiers already
	// logged, so a bucket full of an unknown class logs one line, not one
	// per object. FromS3 runs on every ingest worker concurrently.
	warned sync.Map
	Tiers  []Info
}

// NewMapping creates a new tier mapping with all supported tiers.
func NewMapping() *Mapping {
	all := AllTiers()
	m := &Mapping{
		Tiers:         make([]Info, len(all)),
		indexByS3Name: make(map[string]ID),
	}
	copy(m.Tiers, all)

	for _, t := range m.Tiers {
		m.indexByS3Name[t.Name] = t.ID
	}

	// Add INTELLIGENT_TIERING as alias for ITFrequent (default when no access tier specified).
	m.indexByS3Name["INTELLIGENT_TIERING"] = ITFrequent

	return m
}

// FromS3 maps S3 inventory StorageClass and IntelligentTieringAccessTier to a tier ID.
// If storageClass is INTELLIGENT_TIERING, accessTier determines the specific IT tier.
// If accessTier is empty for IT, defaults to FREQUENT.
func (m *Mapping) FromS3(storageClass, accessTier string) ID {
	if id, ok := canonicalID(storageClass, accessTier); ok {
		return id
	}

	return m.fromS3Normalized(storageClass, accessTier)
}

// canonicalID classifies the exact spellings S3 Inventory writes, which
// is nearly every row, without trimming, upper-casing or a map lookup.
// Anything else misses and goes to fromS3Normalized.
func canonicalID(storageClass, accessTier string) (ID, bool) {
	switch storageClass {
	case "STANDARD":
		return Standard, true
	case "STANDARD_IA":
		return StandardIA, true
	case "ONEZONE_IA":
		return OneZoneIA, true
	case "GLACIER_IR":
		return GlacierIR, true
	case "GLACIER":
		return GlacierFR, true
	case deepArchive:
		return DeepArchive, true
	case "REDUCED_REDUNDANCY":
		return ReducedRedundancy, true
	case "INTELLIGENT_TIERING":
		switch accessTier {
		case "", "FREQUENT_ACCESS", "FREQUENT":
			return ITFrequent, true
		case "INFREQUENT_ACCESS", "INFREQUENT":
			return ITInfrequent, true
		case "ARCHIVE_INSTANT_ACCESS":
			return ITArchiveInstant, true
		case "ARCHIVE_ACCESS", "ARCHIVE":
			return ITArchive, true
		case "DEEP_ARCHIVE_ACCESS", deepArchive:
			return ITDeepArchive, true
		}
	}

	return 0, false
}

// fromS3Normalized is FromS3 for any spelling: it trims and upper-cases
// both fields and logs, once each, values it does not know.
func (m *Mapping) fromS3Normalized(storageClass, accessTier string) ID {
	storageClass = strings.ToUpper(strings.TrimSpace(storageClass))
	accessTier = strings.ToUpper(strings.TrimSpace(accessTier))

	if id, ok := canonicalID(storageClass, accessTier); ok {
		return id
	}

	if storageClass == "INTELLIGENT_TIERING" {
		// The access tier is non-empty and unknown here. Surface it —
		// silent misclassification would inflate Frequent counts.
		if _, seen := m.warned.LoadOrStore("access_tier:"+accessTier, struct{}{}); !seen {
			logging.L().Warn().
				Str("access_tier", accessTier).
				Msg("tiers.FromS3: unknown Intelligent-Tiering access tier, counting as FREQUENT (logged once)")
		}

		return ITFrequent
	}

	if id, ok := m.indexByS3Name[storageClass]; ok {
		return id
	}

	if storageClass != "" {
		if _, seen := m.warned.LoadOrStore("storage_class:"+storageClass, struct{}{}); !seen {
			logging.L().Warn().
				Str("storage_class", storageClass).
				Msg("tiers.FromS3: unknown storage class, counting as STANDARD (logged once)")
		}
	}

	return Standard
}

// Resolve adjusts a classification based on object size. Intelligent-
// Tiering Frequent objects smaller than 128 KiB are reclassified as
// ITFrequentSmall: they are billed at the Frequent Access rate but do
// not incur the monitoring fee and never auto-tier.
func Resolve(id ID, size uint64) ID {
	if id == ITFrequent && size < SmallObjectThresholdBytes {
		return ITFrequentSmall
	}

	return id
}

// ByID returns tier info by ID. The bool is false when id is out of range.
func (m *Mapping) ByID(id ID) (Info, bool) {
	if int(id) < len(m.Tiers) {
		return m.Tiers[id], true
	}

	return Info{}, false
}

// Manifest is written to tiers.json in the index directory.
type Manifest struct {
	Tiers []Info `json:"tiers"`
}

// WriteManifest writes tiers.json with only the tiers that have data.
func WriteManifest(dir string, presentTiers []ID) error {
	mapping := NewMapping()
	manifest := Manifest{
		Tiers: make([]Info, 0, len(presentTiers)),
	}
	for _, id := range presentTiers {
		info, ok := mapping.ByID(id)
		if !ok {
			return fmt.Errorf("%w: %d", ErrUnknownTierID, id)
		}
		manifest.Tiers = append(manifest.Tiers, info)
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal tier manifest: %w", err)
	}

	path := filepath.Join(dir, "tiers.json")
	const tierManifestMode = 0o600
	if err := os.WriteFile(path, data, tierManifestMode); err != nil {
		return fmt.Errorf("write tier manifest: %w", err)
	}

	return nil
}

// ReadManifest reads tiers.json from the index directory. Returns an
// empty Manifest (zero Tiers) when the file is missing so callers
// can dispatch on len(manifest.Tiers) rather than nil-checking.
func ReadManifest(dir string) (*Manifest, error) {
	path := filepath.Join(dir, "tiers.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Manifest{}, nil
		}

		return nil, fmt.Errorf("read tier manifest: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse tier manifest: %w", err)
	}

	return &manifest, nil
}
