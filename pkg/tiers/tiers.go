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
	ExpressOneZone
	Outposts
	Snow
	NumTiers // Sentinel value for array sizing
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
	{ID: ExpressOneZone, Name: "EXPRESS_ONEZONE", FilePrefix: "express_onezone"},
	{ID: Outposts, Name: "OUTPOSTS", FilePrefix: "outposts"},
	{ID: Snow, Name: "SNOW", FilePrefix: "snow"},
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

// ErrUnknownStorageClass is returned by FromS3 for a storage class
// outside the values AWS documents for S3 Inventory.
var ErrUnknownStorageClass = errors.New("unknown storage class")

// ErrUnknownAccessTier is returned by FromS3 for an Intelligent-Tiering
// access tier outside the values AWS documents for S3 Inventory.
var ErrUnknownAccessTier = errors.New("unknown Intelligent-Tiering access tier")

// Mapping provides tier metadata by ID.
type Mapping struct {
	Tiers []Info
}

// NewMapping creates a new tier mapping with all supported tiers.
func NewMapping() *Mapping {
	return &Mapping{Tiers: AllTiers()}
}

// FromS3 maps an S3 Inventory StorageClass and
// IntelligentTieringAccessTier to a tier ID, in any letter case and with
// surrounding whitespace. An empty storage class is STANDARD (the
// inventory has no StorageClass field), and INTELLIGENT_TIERING with an
// empty access tier is FREQUENT. Any other value AWS does not document
// returns ErrUnknownStorageClass or ErrUnknownAccessTier: counting it
// under another tier would make the per-tier stats wrong.
func FromS3(storageClass, accessTier string) (ID, error) {
	if id, ok := canonicalID(storageClass, accessTier); ok {
		return id, nil
	}

	return fromS3Normalized(storageClass, accessTier)
}

// canonicalID classifies the exact spellings S3 Inventory writes, which
// is nearly every row, without trimming, upper-casing or a map lookup.
// Anything else misses and goes to fromS3Normalized.
func canonicalID(storageClass, accessTier string) (ID, bool) {
	switch storageClass {
	case "STANDARD", "":
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
	case "EXPRESS_ONEZONE":
		return ExpressOneZone, true
	case "OUTPOSTS":
		return Outposts, true
	case "SNOW":
		return Snow, true
	case intelligentTiering:
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

// intelligentTiering is the storage class whose objects the access tier
// classifies.
const intelligentTiering = "INTELLIGENT_TIERING"

// maxS3NameLen is more than the longest storage class or access tier
// canonicalID knows, and at most the 32 bytes Go converts a
// non-escaping []byte to a string in without allocating.
const maxS3NameLen = 32

// fromS3Normalized is FromS3 for any spelling: it trims and upper-cases
// both fields into stack buffers, so a known value does not allocate.
func fromS3Normalized(storageClass, accessTier string) (ID, error) {
	var scBuf, atBuf [maxS3NameLen]byte
	sc, ok := normalizeS3Name(&scBuf, storageClass)
	if !ok {
		return 0, fmt.Errorf("%w %q", ErrUnknownStorageClass, storageClass)
	}
	if string(sc) != intelligentTiering {
		if id, ok := canonicalID(string(sc), ""); ok {
			return id, nil
		}

		return 0, fmt.Errorf("%w %q", ErrUnknownStorageClass, storageClass)
	}
	if at, ok := normalizeS3Name(&atBuf, accessTier); ok {
		if id, ok := canonicalID(intelligentTiering, string(at)); ok {
			return id, nil
		}
	}

	return 0, fmt.Errorf("%w %q", ErrUnknownAccessTier, accessTier)
}

// normalizeS3Name writes s, trimmed and with ASCII letters upper-cased,
// into buf. False when it does not fit, so is no known value.
func normalizeS3Name(buf *[maxS3NameLen]byte, s string) ([]byte, bool) {
	s = strings.TrimSpace(s)
	if len(s) > len(buf) {
		return nil, false
	}
	out := buf[:len(s)]
	for i := range len(s) {
		c := s[i]
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		out[i] = c
	}

	return out, true
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

// ManifestFile is the tier manifest's name in the index directory.
const ManifestFile = "tiers.json"

// Manifest is written to tiers.json in the index directory.
type Manifest struct {
	Tiers []Info `json:"tiers"`
}

// EncodeManifest returns the contents of tiers.json listing only the
// tiers that have data.
func EncodeManifest(presentTiers []ID) ([]byte, error) {
	mapping := NewMapping()
	manifest := Manifest{
		Tiers: make([]Info, 0, len(presentTiers)),
	}
	for _, id := range presentTiers {
		info, ok := mapping.ByID(id)
		if !ok {
			return nil, fmt.Errorf("%w: %d", ErrUnknownTierID, id)
		}
		manifest.Tiers = append(manifest.Tiers, info)
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal tier manifest: %w", err)
	}

	return data, nil
}

// ReadManifest reads tiers.json from the index directory. Returns an
// empty Manifest (zero Tiers) when the file is missing so callers
// can dispatch on len(manifest.Tiers) rather than nil-checking.
func ReadManifest(dir string) (*Manifest, error) {
	path := filepath.Join(dir, ManifestFile)
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
