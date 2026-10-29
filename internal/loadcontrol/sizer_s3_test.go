//go:build integration

package loadcontrol_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loadcontrol"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexstore"
)

// TestManifestSizer_IndexSize: a published index's size is the sum of
// its manifest's file sizes; a key that isn't a manifest, or a run with
// no manifest, has none.
func TestManifestSizer_IndexSize(t *testing.T) {
	client := s3test.FetchClient(t)
	bucket := s3test.Bucket(t, client.Raw())
	const key = "idx/src-bucket/daily/2026-10-24T01-00Z/manifest.json"
	body := `{"version":` + strconv.Itoa(format.ManifestVersion) + `,"created_at":"2026-10-24T02:00:00Z","node_count":3,` +
		`"files":{"core_stats.bin":{"size":300,"checksum":"x"},"mph.bin":{"size":40,"checksum":"y"}}}`
	if _, err := client.Raw().PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(body),
	}); err != nil {
		t.Fatal(err)
	}
	sizer := loadcontrol.NewManifestSizer(client)

	want := inventory.ManifestSize{FileFormat: inventory.IndexFileFormat, DataBytes: 340}
	if got, err := sizer.IndexSize(t.Context(), bucket, key); err != nil || got != want {
		t.Errorf("IndexSize = %+v, %v; want %+v", got, err, want)
	}
	if _, err := sizer.IndexSize(t.Context(), bucket, "idx/src-bucket/daily/2026-10-24T01-00Z/core_stats.bin"); err == nil {
		t.Error("IndexSize of a non-manifest key succeeded")
	}
	missing := "idx/src-bucket/daily/2026-10-25T01-00Z/manifest.json"
	if _, err := sizer.IndexSize(t.Context(), bucket, missing); !errors.Is(err, indexstore.ErrNotPublished) {
		t.Errorf("IndexSize of an unpublished run err = %v, want ErrNotPublished", err)
	}
}
