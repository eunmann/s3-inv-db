package tiers

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFromS3_StandardClasses(t *testing.T) {
	m := NewMapping()

	tests := []struct {
		storageClass string
		accessTier   string
		wantID       ID
	}{
		{"STANDARD", "", Standard},
		{"standard", "", Standard},
		{"STANDARD_IA", "", StandardIA},
		{"ONEZONE_IA", "", OneZoneIA},
		{"GLACIER_IR", "", GlacierIR},
		{"GLACIER", "", GlacierFR},
		{"DEEP_ARCHIVE", "", DeepArchive},
		{"REDUCED_REDUNDANCY", "", ReducedRedundancy},
	}

	for _, tt := range tests {
		t.Run(tt.storageClass, func(t *testing.T) {
			got := m.FromS3(tt.storageClass, tt.accessTier)
			if got != tt.wantID {
				t.Errorf("FromS3(%q, %q) = %d, want %d", tt.storageClass, tt.accessTier, got, tt.wantID)
			}
		})
	}
}

func TestFromS3_IntelligentTiering(t *testing.T) {
	m := NewMapping()

	tests := []struct {
		accessTier string
		wantID     ID
	}{
		{"FREQUENT_ACCESS", ITFrequent},
		{"FREQUENT", ITFrequent},
		{"INFREQUENT_ACCESS", ITInfrequent},
		{"INFREQUENT", ITInfrequent},
		{"ARCHIVE_INSTANT_ACCESS", ITArchiveInstant},
		{"ARCHIVE_ACCESS", ITArchive},
		{"ARCHIVE", ITArchive},
		{"DEEP_ARCHIVE_ACCESS", ITDeepArchive},
		{"DEEP_ARCHIVE", ITDeepArchive},
		{"", ITFrequent},        // Missing defaults to Frequent
		{"UNKNOWN", ITFrequent}, // Unknown defaults to Frequent
	}

	for _, tt := range tests {
		t.Run(tt.accessTier, func(t *testing.T) {
			got := m.FromS3("INTELLIGENT_TIERING", tt.accessTier)
			if got != tt.wantID {
				t.Errorf("FromS3(IT, %q) = %d, want %d", tt.accessTier, got, tt.wantID)
			}
		})
	}
}

func TestFromS3_CaseInsensitive(t *testing.T) {
	m := NewMapping()

	tests := []struct {
		storageClass string
		accessTier   string
		wantID       ID
	}{
		{"standard", "", Standard},
		{"Standard", "", Standard},
		{"STANDARD", "", Standard},
		{"intelligent_tiering", "frequent_access", ITFrequent},
		{"Intelligent_Tiering", "Frequent_Access", ITFrequent},
	}

	for _, tt := range tests {
		t.Run(tt.storageClass+"/"+tt.accessTier, func(t *testing.T) {
			got := m.FromS3(tt.storageClass, tt.accessTier)
			if got != tt.wantID {
				t.Errorf("FromS3(%q, %q) = %d, want %d", tt.storageClass, tt.accessTier, got, tt.wantID)
			}
		})
	}
}

func TestFromS3_UnknownClass(t *testing.T) {
	m := NewMapping()

	// Unknown storage class should default to Standard
	got := m.FromS3("UNKNOWN_CLASS", "")
	if got != Standard {
		t.Errorf("FromS3(UNKNOWN_CLASS, '') = %d, want %d (Standard)", got, Standard)
	}
}

func TestFromS3_Whitespace(t *testing.T) {
	m := NewMapping()

	// Should handle whitespace
	got := m.FromS3("  STANDARD  ", "")
	if got != Standard {
		t.Errorf("FromS3 with whitespace = %d, want %d", got, Standard)
	}
}

func TestByID(t *testing.T) {
	m := NewMapping()

	info := m.ByID(Standard)
	if info.Name != "STANDARD" {
		t.Errorf("ByID(Standard).Name = %q, want STANDARD", info.Name)
	}
	if info.FilePrefix != "standard" {
		t.Errorf("ByID(Standard).FilePrefix = %q, want standard", info.FilePrefix)
	}

	info = m.ByID(ITFrequent)
	if info.FilePrefix != "it_frequent" {
		t.Errorf("ByID(ITFrequent).FilePrefix = %q, want it_frequent", info.FilePrefix)
	}
}

func TestWriteReadManifest(t *testing.T) {
	dir := t.TempDir()

	presentTiers := []ID{Standard, GlacierFR, ITFrequent}
	if err := WriteManifest(dir, presentTiers); err != nil {
		t.Fatalf("WriteManifest failed: %v", err)
	}

	// Verify file exists
	path := filepath.Join(dir, "tiers.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("tiers.json not created: %v", err)
	}

	// Read it back
	manifest, err := ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest failed: %v", err)
	}

	if len(manifest.Tiers) != 3 {
		t.Errorf("got %d tiers, want 3", len(manifest.Tiers))
	}

	// Verify tier IDs
	wantIDs := map[ID]bool{Standard: true, GlacierFR: true, ITFrequent: true}
	for _, tier := range manifest.Tiers {
		if !wantIDs[tier.ID] {
			t.Errorf("unexpected tier ID %d in manifest", tier.ID)
		}
	}
}

func TestReadManifest_NotExists(t *testing.T) {
	dir := t.TempDir()

	manifest, err := ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest failed: %v", err)
	}
	if manifest != nil {
		t.Error("expected nil manifest for missing file")
	}
}

func TestAllTiersComplete(t *testing.T) {
	// Verify all tier IDs from 0 to NumTiers-1 are covered
	if len(AllTiers) != int(NumTiers) {
		t.Errorf("AllTiers has %d entries, expected %d", len(AllTiers), NumTiers)
	}

	for i, tier := range AllTiers {
		if int(tier.ID) != i {
			t.Errorf("AllTiers[%d].ID = %d, expected %d", i, tier.ID, i)
		}
	}
}
