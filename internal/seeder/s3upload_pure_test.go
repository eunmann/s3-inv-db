package seeder_test

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/benchutil"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

func TestAutoChunkCount(t *testing.T) {
	cases := []struct {
		objects int
		want    int
	}{
		{0, 1},
		{50_000, 1},
		{99_999, 1},
		{100_000, 1},
		{100_001, 1},
		{250_000, 2},
		{500_000, 5},
		{2_500_000, 16},
		{100_000_000, 16},
	}
	for _, c := range cases {
		if got := seeder.AutoChunkCount(c.objects); got != c.want {
			t.Errorf("AutoChunkCount(%d) = %d, want %d", c.objects, got, c.want)
		}
	}
}

func TestS3Config_Validate(t *testing.T) {
	cases := []struct {
		want error
		cfg  seeder.S3Config
		name string
	}{
		{name: "ok", cfg: seeder.S3Config{Bucket: "b", SrcBucket: "s", Prefix: "p/"}, want: nil},
		{name: "ok empty prefix", cfg: seeder.S3Config{Bucket: "b", SrcBucket: "s"}, want: nil},
		{name: "missing bucket", cfg: seeder.S3Config{SrcBucket: "s"}, want: seeder.ErrEmptyBucket},
		{name: "missing src bucket", cfg: seeder.S3Config{Bucket: "b"}, want: seeder.ErrEmptySrcBucket},
		{name: "prefix without trailing slash", cfg: seeder.S3Config{Bucket: "b", SrcBucket: "s", Prefix: "p"}, want: seeder.ErrPrefixNotSlash},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.cfg.Validate()
			if !errors.Is(err, c.want) {
				t.Errorf("Validate() = %v, want %v", err, c.want)
			}
		})
	}
}

func TestMd5Hex_KnownVectors(t *testing.T) {
	// RFC 1321 test vectors.
	cases := map[string]string{
		"":    "d41d8cd98f00b204e9800998ecf8427e",
		"abc": "900150983cd24fb0d6963f7d28e17f72",
	}
	for input, want := range cases {
		if got := seeder.MD5Hex([]byte(input)); got != want {
			t.Errorf("md5Hex(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestRunStampUUID_DeterministicPerRunAndIndex(t *testing.T) {
	stamp := time.Date(2026, 5, 13, 3, 2, 1, 0, time.UTC)
	a := seeder.RunStampUUID(stamp, 1)
	b := seeder.RunStampUUID(stamp, 1)
	if a != b {
		t.Errorf("same (stamp, index) produced different uuids: %q vs %q", a, b)
	}
	c := seeder.RunStampUUID(stamp, 2)
	if a == c {
		t.Errorf("different index produced same uuid: %q", a)
	}
	d := seeder.RunStampUUID(stamp.Add(time.Hour), 1)
	if a == d {
		t.Errorf("different stamp produced same uuid: %q", a)
	}
	if got := len(a); got != 15 {
		t.Errorf("uuid len = %d, want 15 (got %q)", got, a)
	}
	if !strings.Contains(a, "-") {
		t.Errorf("uuid missing dash separator: %q", a)
	}
}

func TestTierToS3Columns_IntelligentTiering(t *testing.T) {
	m := tiers.NewMapping()
	cases := []struct {
		wantClass string
		wantTier  string
		id        tiers.ID
	}{
		{id: tiers.ITFrequent, wantClass: "INTELLIGENT_TIERING", wantTier: "FREQUENT_ACCESS"},
		{id: tiers.ITInfrequent, wantClass: "INTELLIGENT_TIERING", wantTier: "INFREQUENT_ACCESS"},
		{id: tiers.ITArchiveInstant, wantClass: "INTELLIGENT_TIERING", wantTier: "ARCHIVE_INSTANT_ACCESS"},
		{id: tiers.ITArchive, wantClass: "INTELLIGENT_TIERING", wantTier: "ARCHIVE_ACCESS"},
		{id: tiers.ITDeepArchive, wantClass: "INTELLIGENT_TIERING", wantTier: "DEEP_ARCHIVE_ACCESS"},
	}
	for _, c := range cases {
		got, err := seeder.TierToS3Columns(m, c.id)
		if err != nil {
			t.Errorf("tierToS3Columns(%v): %v", c.id, err)

			continue
		}
		if got.StorageClass != c.wantClass || got.AccessTier != c.wantTier {
			t.Errorf("tierToS3Columns(%v) = (%q, %q), want (%q, %q)",
				c.id, got.StorageClass, got.AccessTier, c.wantClass, c.wantTier)
		}
	}
}

func TestTierToS3Columns_NonIntelligentLeavesAccessTierEmpty(t *testing.T) {
	m := tiers.NewMapping()
	for _, id := range []tiers.ID{tiers.Standard, tiers.StandardIA, tiers.GlacierFR} {
		got, err := seeder.TierToS3Columns(m, id)
		if err != nil {
			t.Errorf("tierToS3Columns(%v): %v", id, err)

			continue
		}
		if got.AccessTier != "" {
			t.Errorf("non-IT tier %v produced AccessTier %q, want empty", id, got.AccessTier)
		}
		if got.StorageClass == "" {
			t.Errorf("non-IT tier %v produced empty StorageClass", id)
		}
	}
}

func TestEncodeCSVGz_RoundTripsRows(t *testing.T) {
	objects := []benchutil.FakeObject{
		{Key: "a/1", Size: 100, TierID: tiers.Standard},
		{Key: "a/2", Size: 250, TierID: tiers.ITFrequent},
		{Key: "b/3", Size: 9999, TierID: tiers.GlacierFR},
	}
	payload, err := seeder.EncodeCSVGz("src-bucket", objects)
	if err != nil {
		t.Fatalf("encodeCSVGz: %v", err)
	}
	if payload.Size != int64(len(payload.Body)) {
		t.Errorf("size = %d, want %d (len(body))", payload.Size, len(payload.Body))
	}
	if len(payload.Body) == 0 {
		t.Fatal("body is empty")
	}

	gr, err := gzip.NewReader(bytes.NewReader(payload.Body))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gr.Close()
	raw, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("read decompressed: %v", err)
	}
	r := csv.NewReader(bytes.NewReader(raw))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("csv.ReadAll: %v", err)
	}
	if len(rows) != len(objects) {
		t.Fatalf("rows = %d, want %d", len(rows), len(objects))
	}
	for i, row := range rows {
		if row[0] != "src-bucket" {
			t.Errorf("row %d source bucket = %q, want src-bucket", i, row[0])
		}
		if row[1] != objects[i].Key {
			t.Errorf("row %d key = %q, want %q", i, row[1], objects[i].Key)
		}
	}
}
