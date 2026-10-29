package tiers_test

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// s3Value is a storage class / access tier pair S3 Inventory writes and
// the tier it maps to.
type s3Value struct {
	storageClass, accessTier string
	want                     tiers.ID
}

// documentedValues is every storage class AWS documents for S3
// Inventory, and every Intelligent-Tiering access tier (with the short
// forms FromS3 also accepts), plus the empty values of an inventory
// without those fields.
func documentedValues() []s3Value {
	return []s3Value{
		{"STANDARD", "", tiers.Standard},
		{"REDUCED_REDUNDANCY", "", tiers.ReducedRedundancy},
		{"STANDARD_IA", "", tiers.StandardIA},
		{"ONEZONE_IA", "", tiers.OneZoneIA},
		{"GLACIER", "", tiers.GlacierFR},
		{"DEEP_ARCHIVE", "", tiers.DeepArchive},
		{"OUTPOSTS", "", tiers.Outposts},
		{"GLACIER_IR", "", tiers.GlacierIR},
		{"SNOW", "", tiers.Snow},
		{"EXPRESS_ONEZONE", "", tiers.ExpressOneZone},
		{"", "", tiers.Standard},
		{"INTELLIGENT_TIERING", "", tiers.ITFrequent},
		{"INTELLIGENT_TIERING", "FREQUENT_ACCESS", tiers.ITFrequent},
		{"INTELLIGENT_TIERING", "FREQUENT", tiers.ITFrequent},
		{"INTELLIGENT_TIERING", "INFREQUENT_ACCESS", tiers.ITInfrequent},
		{"INTELLIGENT_TIERING", "INFREQUENT", tiers.ITInfrequent},
		{"INTELLIGENT_TIERING", "ARCHIVE_INSTANT_ACCESS", tiers.ITArchiveInstant},
		{"INTELLIGENT_TIERING", "ARCHIVE_ACCESS", tiers.ITArchive},
		{"INTELLIGENT_TIERING", "ARCHIVE", tiers.ITArchive},
		{"INTELLIGENT_TIERING", "DEEP_ARCHIVE_ACCESS", tiers.ITDeepArchive},
		{"INTELLIGENT_TIERING", "DEEP_ARCHIVE", tiers.ITDeepArchive},
	}
}

// TestFromS3_DocumentedValues: every documented value maps to its tier
// as S3 writes it, in lower case, in mixed case and padded.
func TestFromS3_DocumentedValues(t *testing.T) {
	for _, v := range documentedValues() {
		for _, sc := range spellings(v.storageClass) {
			for _, at := range spellings(v.accessTier) {
				got, err := tiers.FromS3(sc, at)
				if err != nil || got != v.want {
					t.Errorf("FromS3(%q, %q) = %d, %v; want %d", sc, at, got, err, v.want)
				}
			}
		}
	}
}

// TestFromS3_AccessTierOnlyForIntelligentTiering: the access tier of any
// other storage class is not read.
func TestFromS3_AccessTierOnlyForIntelligentTiering(t *testing.T) {
	for _, at := range []string{"ARCHIVE_ACCESS", "UNKNOWN", strings.Repeat("x", 100)} {
		for _, sc := range []string{"GLACIER", "glacier", ""} {
			want := tiers.GlacierFR
			if sc == "" {
				want = tiers.Standard
			}
			if got, err := tiers.FromS3(sc, at); err != nil || got != want {
				t.Errorf("FromS3(%q, %q) = %d, %v; want %d", sc, at, got, err, want)
			}
		}
	}
}

// TestFromS3_UnknownValuesFail: a value AWS does not document is an
// error naming it, never a guess at a tier.
func TestFromS3_UnknownValuesFail(t *testing.T) {
	cases := []struct {
		storageClass, accessTier string
		want                     error
	}{
		{"UNKNOWN_CLASS", "", tiers.ErrUnknownStorageClass},
		{"standard ia", "", tiers.ErrUnknownStorageClass},
		{"GLACIER_FR", "", tiers.ErrUnknownStorageClass},
		{"INTELLIGENT_TIERING_FREQUENT", "", tiers.ErrUnknownStorageClass},
		{"STANDARD\x00", "", tiers.ErrUnknownStorageClass},
		{strings.Repeat("STANDARD", 5), "", tiers.ErrUnknownStorageClass},
		{"INTELLIGENT_TIERING", "UNKNOWN", tiers.ErrUnknownAccessTier},
		{"intelligent_tiering", "archive_instant", tiers.ErrUnknownAccessTier},
		{"INTELLIGENT_TIERING", strings.Repeat("A", 40), tiers.ErrUnknownAccessTier},
	}
	for _, c := range cases {
		id, err := tiers.FromS3(c.storageClass, c.accessTier)
		if !errors.Is(err, c.want) {
			t.Errorf("FromS3(%q, %q) = %d, %v; want %v", c.storageClass, c.accessTier, id, err, c.want)

			continue
		}
		value := c.storageClass
		if errors.Is(c.want, tiers.ErrUnknownAccessTier) {
			value = c.accessTier
		}
		if quoted := strconv.Quote(value); !strings.Contains(err.Error(), quoted) {
			t.Errorf("FromS3(%q, %q) error %q does not name %s", c.storageClass, c.accessTier, err, quoted)
		}
	}
}

// TestFromS3_KnownValuesDoNotAllocate: FromS3 runs once per inventory
// row, so classifying a known value in any spelling must not allocate.
func TestFromS3_KnownValuesDoNotAllocate(t *testing.T) {
	var pairs [][2]string
	for _, v := range documentedValues() {
		for _, sc := range spellings(v.storageClass) {
			pairs = append(pairs, [2]string{sc, strings.ToLower(v.accessTier)})
		}
	}
	allocs := testing.AllocsPerRun(10, func() {
		for _, p := range pairs {
			if _, err := tiers.FromS3(p[0], p[1]); err != nil {
				t.Fatal(err)
			}
		}
	})
	if allocs != 0 {
		t.Errorf("FromS3 over %d known values: %v allocs, want 0", len(pairs), allocs)
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

func TestEncodeReadManifest(t *testing.T) {
	dir := t.TempDir()

	presentTiers := []tiers.ID{tiers.Standard, tiers.GlacierFR, tiers.ITFrequent}
	data, err := tiers.EncodeManifest(presentTiers)
	if err != nil {
		t.Fatalf("EncodeManifest failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, tiers.ManifestFile), data, 0o600); err != nil {
		t.Fatal(err)
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
	if s == "" {
		return []string{"", " ", "\t"}
	}

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
	out := spellings("")
	for _, s := range names {
		out = append(out, spellings(s)...)
	}

	return out
}

func TestFromS3_FastPathMatchesNormalized(t *testing.T) {
	all := tiers.AllTiers()
	classes := make([]string, 0, len(all)+3)
	for _, info := range all {
		classes = append(classes, info.Name)
	}
	classes = append(classes, "INTELLIGENT_TIERING", "UNKNOWN_CLASS", "X")
	accessTiers := []string{
		"FREQUENT_ACCESS", "FREQUENT", "INFREQUENT_ACCESS", "INFREQUENT",
		"ARCHIVE_INSTANT_ACCESS", "ARCHIVE_ACCESS", "ARCHIVE",
		"DEEP_ARCHIVE_ACCESS", "DEEP_ARCHIVE", "UNKNOWN", "X",
	}

	for _, sc := range allSpellings(classes) {
		for _, at := range allSpellings(accessTiers) {
			got, gotErr := tiers.FromS3(sc, at)
			want, wantErr := tiers.FromS3Normalized(sc, at)
			if got != want || errString(gotErr) != errString(wantErr) {
				t.Errorf("FromS3(%q, %q) = %d, %v; normalizing path gives %d, %v", sc, at, got, gotErr, want, wantErr)
			}
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
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
		{"EXPRESS_ONEZONE", "", tiers.ExpressOneZone},
		{"OUTPOSTS", "", tiers.Outposts},
		{"SNOW", "", tiers.Snow},
		{"", "", tiers.Standard},
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
		{" ", ""},
	}
	for _, c := range misses {
		if _, ok := tiers.CanonicalID(c[0], c[1]); ok {
			t.Errorf("CanonicalID(%q, %q) hit; want a miss so the normalizing path handles it", c[0], c[1])
		}
	}
}
