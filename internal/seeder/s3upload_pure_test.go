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

func TestS3Config_Validate(t *testing.T) {
	cases := []struct {
		name string
		cfg  seeder.S3Config
		want error
	}{
		{"ok", seeder.S3Config{Bucket: "b", SrcBucket: "s", Prefix: "p/"}, nil},
		{"ok empty prefix", seeder.S3Config{Bucket: "b", SrcBucket: "s"}, nil},
		{"missing bucket", seeder.S3Config{SrcBucket: "s"}, seeder.ErrEmptyBucket},
		{"missing src bucket", seeder.S3Config{Bucket: "b"}, seeder.ErrEmptySrcBucket},
		{"prefix without trailing slash", seeder.S3Config{Bucket: "b", SrcBucket: "s", Prefix: "p"}, seeder.ErrPrefixNotSlash},
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
		id                  tiers.ID
		wantClass, wantTier string
	}{
		{tiers.ITFrequent, "INTELLIGENT_TIERING", "FREQUENT_ACCESS"},
		{tiers.ITInfrequent, "INTELLIGENT_TIERING", "INFREQUENT_ACCESS"},
		{tiers.ITArchiveInstant, "INTELLIGENT_TIERING", "ARCHIVE_INSTANT_ACCESS"},
		{tiers.ITArchive, "INTELLIGENT_TIERING", "ARCHIVE_ACCESS"},
		{tiers.ITDeepArchive, "INTELLIGENT_TIERING", "DEEP_ARCHIVE_ACCESS"},
	}
	for _, c := range cases {
		got := seeder.TierToS3Columns(m, c.id)
		if got.StorageClass != c.wantClass || got.AccessTier != c.wantTier {
			t.Errorf("tierToS3Columns(%v) = (%q, %q), want (%q, %q)",
				c.id, got.StorageClass, got.AccessTier, c.wantClass, c.wantTier)
		}
	}
}

func TestTierToS3Columns_NonIntelligentLeavesAccessTierEmpty(t *testing.T) {
	m := tiers.NewMapping()
	for _, id := range []tiers.ID{tiers.Standard, tiers.StandardIA, tiers.GlacierFR} {
		got := seeder.TierToS3Columns(m, id)
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
