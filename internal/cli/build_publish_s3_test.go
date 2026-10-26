//go:build integration

package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/cli"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/indexstore"
	"github.com/rs/zerolog"
)

// TestBuildPublishesToS3: build --out s3://... Publishes the index under
// <source-bucket>/<inventory-id>/<run>/ with its manifest, leaves no
// work directory behind, and the published index fetches and opens with
// the inventory's object count.
func TestBuildPublishesToS3(t *testing.T) {
	raw := s3test.RawClient(t)
	bucket := s3test.Bucket(t, raw)
	stamp := time.Date(2026, 10, 24, 1, 0, 0, 0, time.UTC)
	info, err := seeder.UploadInventory(t.Context(), raw,
		seeder.Config{Target: seeder.TargetS3, Objects: 2000, Preset: "realistic", Seed: 7, Logger: zerolog.Nop()},
		seeder.S3Config{Bucket: bucket, Prefix: "inventory/", SrcBucket: "data-bucket"}, 1, 7, stamp)
	if err != nil {
		t.Fatal(err)
	}
	manifest := info.Path
	if !strings.HasPrefix(manifest, "s3://") {
		manifest = "s3://" + bucket + "/" + strings.TrimPrefix(manifest, "/")
	}
	key := strings.TrimPrefix(manifest, "s3://"+bucket+"/")
	run, err := indexstore.RunFromManifestKey(key)
	if err != nil {
		t.Fatalf("seeded manifest %q: %v", manifest, err)
	}

	work := t.TempDir()
	if err := cli.Run([]string{"build", "--s3-manifest", manifest, "--out", "s3://" + bucket + "/indexes/", "--work-dir", work}); err != nil {
		t.Fatalf("build: %v", err)
	}
	if left, _ := os.ReadDir(work); len(left) != 0 {
		t.Errorf("work dir left %v", left)
	}
	dst := filepath.Join(t.TempDir(), "fetched")
	if err := indexstore.New(raw, bucket).Fetch(t.Context(), run.Prefix("indexes/"), dst, nil); err != nil {
		t.Fatalf("fetch the published index: %v", err)
	}
	idx, err := indexread.Open(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if s, ok := idx.StatsForPrefix(""); !ok || s.ObjectCount != 2000 {
		t.Errorf("root = %+v (%v), want 2000 objects", s, ok)
	}
}
