package tiers_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestFromS3_StandardClasses(t *testing.T) {
	m := tiers.NewMapping()

	tests := []struct {
		storageClass string
		accessTier   string
		wantID       tiers.ID
	}{
		{"STANDARD", "", tiers.Standard},
		{"standard", "", tiers.Standard},
		{"STANDARD_IA", "", tiers.StandardIA},
		{"ONEZONE_IA", "", tiers.OneZoneIA},
		{"GLACIER_IR", "", tiers.GlacierIR},
		{"GLACIER", "", tiers.GlacierFR},
		{"DEEP_ARCHIVE", "", tiers.DeepArchive},
		{"REDUCED_REDUNDANCY", "", tiers.ReducedRedundancy},
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
	m := tiers.NewMapping()

	tests := []struct {
		accessTier string
		wantID     tiers.ID
	}{
		{"FREQUENT_ACCESS", tiers.ITFrequent},
		{"FREQUENT", tiers.ITFrequent},
		{"INFREQUENT_ACCESS", tiers.ITInfrequent},
		{"INFREQUENT", tiers.ITInfrequent},
		{"ARCHIVE_INSTANT_ACCESS", tiers.ITArchiveInstant},
		{"ARCHIVE_ACCESS", tiers.ITArchive},
		{"ARCHIVE", tiers.ITArchive},
		{"DEEP_ARCHIVE_ACCESS", tiers.ITDeepArchive},
		{"DEEP_ARCHIVE", tiers.ITDeepArchive},
		{"", tiers.ITFrequent},
		{"UNKNOWN", tiers.ITFrequent},
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
	m := tiers.NewMapping()

	tests := []struct {
		storageClass string
		accessTier   string
		wantID       tiers.ID
	}{
		{"standard", "", tiers.Standard},
		{"Standard", "", tiers.Standard},
		{"STANDARD", "", tiers.Standard},
		{"intelligent_tiering", "frequent_access", tiers.ITFrequent},
		{"Intelligent_Tiering", "Frequent_Access", tiers.ITFrequent},
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
	m := tiers.NewMapping()

	got := m.FromS3("UNKNOWN_CLASS", "")
	if got != tiers.Standard {
		t.Errorf("FromS3(UNKNOWN_CLASS, '') = %d, want %d (Standard)", got, tiers.Standard)
	}
}

func TestFromS3_Whitespace(t *testing.T) {
	m := tiers.NewMapping()

	got := m.FromS3("  STANDARD  ", "")
	if got != tiers.Standard {
		t.Errorf("FromS3 with whitespace = %d, want %d", got, tiers.Standard)
	}
}

func TestByID(t *testing.T) {
	m := tiers.NewMapping()

	info, ok := m.ByID(tiers.Standard)
	if !ok {
		t.Fatal("ByID(Standard) returned ok=false")
	}
	if info.Name != "STANDARD" {
		t.Errorf("ByID(Standard).Name = %q, want STANDARD", info.Name)
	}
	if info.FilePrefix != "standard" {
		t.Errorf("ByID(Standard).FilePrefix = %q, want standard", info.FilePrefix)
	}

	info, ok = m.ByID(tiers.ITFrequent)
	if !ok {
		t.Fatal("ByID(ITFrequent) returned ok=false")
	}
	if info.FilePrefix != "it_frequent" {
		t.Errorf("ByID(ITFrequent).FilePrefix = %q, want it_frequent", info.FilePrefix)
	}

	if _, ok := m.ByID(tiers.NumTiers); ok {
		t.Error("ByID(NumTiers) returned ok=true, want false for out-of-range id")
	}
}

func TestWriteReadManifest(t *testing.T) {
	dir := t.TempDir()

	presentTiers := []tiers.ID{tiers.Standard, tiers.GlacierFR, tiers.ITFrequent}
	if err := tiers.WriteManifest(dir, presentTiers); err != nil {
		t.Fatalf("WriteManifest failed: %v", err)
	}

	path := filepath.Join(dir, "tiers.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("tiers.json not created: %v", err)
	}

	manifest, err := tiers.ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest failed: %v", err)
	}

	if len(manifest.Tiers) != 3 {
		t.Errorf("got %d tiers, want 3", len(manifest.Tiers))
	}

	wantIDs := map[tiers.ID]bool{tiers.Standard: true, tiers.GlacierFR: true, tiers.ITFrequent: true}
	for _, tier := range manifest.Tiers {
		if !wantIDs[tier.ID] {
			t.Errorf("unexpected tier ID %d in manifest", tier.ID)
		}
	}
}

func TestReadManifest_NotExists(t *testing.T) {
	dir := t.TempDir()

	manifest, err := tiers.ReadManifest(dir)
	if err != nil {
		t.Fatalf("ReadManifest failed: %v", err)
	}
	if manifest == nil {
		t.Fatal("expected non-nil empty manifest for missing file")
	}
	if len(manifest.Tiers) != 0 {
		t.Errorf("expected zero tiers when file is missing, got %d", len(manifest.Tiers))
	}
}

func TestResolve_SmallITFrequent(t *testing.T) {
	cases := []struct {
		name string
		size uint64
		id   tiers.ID
		want tiers.ID
	}{
		{name: "small IT frequent -> small bucket", id: tiers.ITFrequent, size: 1024, want: tiers.ITFrequentSmall},
		{name: "IT frequent at threshold stays", id: tiers.ITFrequent, size: tiers.SmallObjectThresholdBytes, want: tiers.ITFrequent},
		{name: "IT frequent above threshold stays", id: tiers.ITFrequent, size: tiers.SmallObjectThresholdBytes + 1, want: tiers.ITFrequent},
		{name: "small Standard untouched", id: tiers.Standard, size: 1024, want: tiers.Standard},
		{name: "small IT infrequent untouched", id: tiers.ITInfrequent, size: 1024, want: tiers.ITInfrequent},
		{name: "zero-byte IT frequent reclassifies", id: tiers.ITFrequent, size: 0, want: tiers.ITFrequentSmall},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tiers.Resolve(tc.id, tc.size); got != tc.want {
				t.Errorf("Resolve(%v, %d) = %v, want %v", tc.id, tc.size, got, tc.want)
			}
		})
	}
}

func TestAllTiersComplete(t *testing.T) {
	all := tiers.AllTiers()
	if len(all) != int(tiers.NumTiers) {
		t.Errorf("AllTiers has %d entries, expected %d", len(all), tiers.NumTiers)
	}

	for i, tier := range all {
		if int(tier.ID) != i {
			t.Errorf("AllTiers[%d].ID = %d, expected %d", i, tier.ID, i)
		}
	}
}

// spellings returns s as written and with the casing and surrounding
// whitespace FromS3 must normalize.
func spellings(s string) []string {
	return []string{
		s,
		strings.ToLower(s),
		strings.ToUpper(s[:1]) + strings.ToLower(s[1:]),
		" " + s,
		s + " ",
		"\t" + s + "\n",
		" " + strings.ToLower(s) + " ",
	}
}

func allSpellings(names []string) []string {
	blanks := []string{"", " ", "\t"}
	out := make([]string, 0, len(blanks)+len(names)*len(spellings("X")))
	out = append(out, blanks...)
	for _, s := range names {
		out = append(out, spellings(s)...)
	}

	return out
}

func TestFromS3_EveryTierNameAnySpelling(t *testing.T) {
	m := tiers.NewMapping()
	for _, info := range tiers.AllTiers() {
		for _, sc := range spellings(info.Name) {
			if got := m.FromS3(sc, ""); got != info.ID {
				t.Errorf("FromS3(%q, \"\") = %d, want %d", sc, got, info.ID)
			}
		}
	}
}

func TestFromS3_FastPathMatchesNormalized(t *testing.T) {
	all := tiers.AllTiers()
	classes := make([]string, 0, len(all)+6)
	for _, info := range all {
		classes = append(classes, info.Name)
	}
	classes = append(classes, "INTELLIGENT_TIERING", "UNKNOWN_CLASS", "OUTPOSTS", "EXPRESS_ONEZONE", "SNOW", "X")
	accessTiers := []string{
		"FREQUENT_ACCESS", "FREQUENT", "INFREQUENT_ACCESS", "INFREQUENT",
		"ARCHIVE_INSTANT_ACCESS", "ARCHIVE_ACCESS", "ARCHIVE",
		"DEEP_ARCHIVE_ACCESS", "DEEP_ARCHIVE", "UNKNOWN", "X",
	}

	m := tiers.NewMapping()
	for _, sc := range allSpellings(classes) {
		for _, at := range allSpellings(accessTiers) {
			if got, want := m.FromS3(sc, at), tiers.FromS3Normalized(m, sc, at); got != want {
				t.Errorf("FromS3(%q, %q) = %d, normalizing path gives %d", sc, at, got, want)
			}
		}
	}
}

func TestFromS3_CanonicalSpellingsTakeFastPath(t *testing.T) {
	hits := []struct {
		storageClass, accessTier string
		want                     tiers.ID
	}{
		{"STANDARD", "", tiers.Standard},
		{"STANDARD_IA", "", tiers.StandardIA},
		{"ONEZONE_IA", "", tiers.OneZoneIA},
		{"GLACIER_IR", "", tiers.GlacierIR},
		{"GLACIER", "", tiers.GlacierFR},
		{"DEEP_ARCHIVE", "", tiers.DeepArchive},
		{"REDUCED_REDUNDANCY", "", tiers.ReducedRedundancy},
		{"INTELLIGENT_TIERING", "", tiers.ITFrequent},
		{"INTELLIGENT_TIERING", "FREQUENT", tiers.ITFrequent},
		{"INTELLIGENT_TIERING", "FREQUENT_ACCESS", tiers.ITFrequent},
		{"INTELLIGENT_TIERING", "INFREQUENT", tiers.ITInfrequent},
		{"INTELLIGENT_TIERING", "INFREQUENT_ACCESS", tiers.ITInfrequent},
		{"INTELLIGENT_TIERING", "ARCHIVE_INSTANT_ACCESS", tiers.ITArchiveInstant},
		{"INTELLIGENT_TIERING", "ARCHIVE", tiers.ITArchive},
		{"INTELLIGENT_TIERING", "ARCHIVE_ACCESS", tiers.ITArchive},
		{"INTELLIGENT_TIERING", "DEEP_ARCHIVE", tiers.ITDeepArchive},
		{"INTELLIGENT_TIERING", "DEEP_ARCHIVE_ACCESS", tiers.ITDeepArchive},
	}
	for _, c := range hits {
		got, ok := tiers.CanonicalID(c.storageClass, c.accessTier)
		if !ok || got != c.want {
			t.Errorf("CanonicalID(%q, %q) = %d, %v; want %d, true", c.storageClass, c.accessTier, got, ok, c.want)
		}
	}

	misses := [][2]string{
		{"standard", ""},
		{" STANDARD", ""},
		{"INTELLIGENT_TIERING", "frequent"},
		{"INTELLIGENT_TIERING", "UNKNOWN"},
		{"UNKNOWN_CLASS", ""},
		{"", ""},
	}
	for _, c := range misses {
		if _, ok := tiers.CanonicalID(c[0], c[1]); ok {
			t.Errorf("CanonicalID(%q, %q) hit; want a miss so the normalizing path handles it", c[0], c[1])
		}
	}
}
