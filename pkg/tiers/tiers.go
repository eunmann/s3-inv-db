// Package tiers defines S3 storage class and Intelligent-Tiering tier mappings.
package tiers

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

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
	NumTiers // Sentinel value for array sizing
)

// Info describes a storage tier.
type Info struct {
	ID         ID     `json:"id"`
	Name       string `json:"name"`
	FilePrefix string `json:"file"`
}

// AllTiers contains information about all supported tiers.
var AllTiers = []Info{
	{Standard, "STANDARD", "standard"},
	{StandardIA, "STANDARD_IA", "standard_ia"},
	{OneZoneIA, "ONEZONE_IA", "onezone_ia"},
	{GlacierIR, "GLACIER_IR", "glacier_ir"},
	{GlacierFR, "GLACIER", "glacier_fr"},
	{DeepArchive, "DEEP_ARCHIVE", "deep_archive"},
	{ReducedRedundancy, "REDUCED_REDUNDANCY", "reduced_redundancy"},
	{ITFrequent, "INTELLIGENT_TIERING_FREQUENT", "it_frequent"},
	{ITInfrequent, "INTELLIGENT_TIERING_INFREQUENT", "it_infrequent"},
	{ITArchiveInstant, "INTELLIGENT_TIERING_ARCHIVE_INSTANT", "it_archive_instant"},
	{ITArchive, "INTELLIGENT_TIERING_ARCHIVE", "it_archive"},
	{ITDeepArchive, "INTELLIGENT_TIERING_DEEP_ARCHIVE", "it_deep_archive"},
}

// Mapping provides tier lookup and metadata.
type Mapping struct {
	Tiers         []Info
	indexByName   map[string]ID
	indexByS3Name map[string]ID
}

// NewMapping creates a new tier mapping with all supported tiers.
func NewMapping() *Mapping {
	m := &Mapping{
		Tiers:         make([]Info, len(AllTiers)),
		indexByName:   make(map[string]ID),
		indexByS3Name: make(map[string]ID),
	}
	copy(m.Tiers, AllTiers)

	for _, t := range m.Tiers {
		m.indexByName[t.Name] = t.ID
		m.indexByS3Name[strings.ToUpper(t.Name)] = t.ID
	}

	// Add INTELLIGENT_TIERING as alias for ITFrequent (default when no access tier specified)
	m.indexByS3Name["INTELLIGENT_TIERING"] = ITFrequent

	return m
}

// FromS3 maps S3 inventory StorageClass and IntelligentTieringAccessTier to a tier ID.
// If storageClass is INTELLIGENT_TIERING, accessTier determines the specific IT tier.
// If accessTier is empty for IT, defaults to FREQUENT.
func (m *Mapping) FromS3(storageClass, accessTier string) ID {
	storageClass = strings.ToUpper(strings.TrimSpace(storageClass))
	accessTier = strings.ToUpper(strings.TrimSpace(accessTier))

	// Handle Intelligent-Tiering with access tier
	if storageClass == "INTELLIGENT_TIERING" {
		switch accessTier {
		case "FREQUENT_ACCESS", "FREQUENT":
			return ITFrequent
		case "INFREQUENT_ACCESS", "INFREQUENT":
			return ITInfrequent
		case "ARCHIVE_INSTANT_ACCESS":
			return ITArchiveInstant
		case "ARCHIVE_ACCESS", "ARCHIVE":
			return ITArchive
		case "DEEP_ARCHIVE_ACCESS", "DEEP_ARCHIVE":
			return ITDeepArchive
		default:
			// Default to Frequent if access tier is missing or unknown
			return ITFrequent
		}
	}

	// Look up standard storage class
	if id, ok := m.indexByS3Name[storageClass]; ok {
		return id
	}

	// Default to Standard for unknown classes
	return Standard
}

// ByID returns tier info by ID.
func (m *Mapping) ByID(id ID) Info {
	if int(id) < len(m.Tiers) {
		return m.Tiers[id]
	}
	return Info{ID: id, Name: "UNKNOWN", FilePrefix: "unknown"}
}

// TierManifest is written to tiers.json in the index directory.
type TierManifest struct {
	Tiers []Info `json:"tiers"`
}

// WriteManifest writes tiers.json with only the tiers that have data.
func WriteManifest(dir string, presentTiers []ID) error {
	mapping := NewMapping()
	manifest := TierManifest{
		Tiers: make([]Info, 0, len(presentTiers)),
	}
	for _, id := range presentTiers {
		manifest.Tiers = append(manifest.Tiers, mapping.ByID(id))
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal tier manifest: %w", err)
	}

	path := filepath.Join(dir, "tiers.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write tier manifest: %w", err)
	}

	return nil
}

// ReadManifest reads tiers.json from the index directory.
func ReadManifest(dir string) (*TierManifest, error) {
	path := filepath.Join(dir, "tiers.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No tier data
		}
		return nil, fmt.Errorf("read tier manifest: %w", err)
	}

	var manifest TierManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse tier manifest: %w", err)
	}

	return &manifest, nil
}
