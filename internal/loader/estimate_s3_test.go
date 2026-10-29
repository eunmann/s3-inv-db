//go:build integration

package loader_test

import (
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loadcontrol"
	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/rs/zerolog"
)

// The disk a load reserves, estimated from the inventory manifest
// before the build, covers the index the pipeline then builds from the
// AWS-like seeded inventories, and is at most 2.5 times it: realistic
// keys (about 2.7 prefixes per object at this size) and deep_pyramid,
// the densest seeded layout (about 7).
func TestEstimatedIndexSize_CoversBuiltIndex(t *testing.T) {
	const objects = 100_000
	fc := s3test.FetchClient(t)
	bucket := s3test.Bucket(t, fc.Raw())
	sizer := loadcontrol.NewManifestSizer(fc)
	stamp := time.Now().UTC().Truncate(time.Minute)

	for i, preset := range []string{"realistic", "deep_pyramid"} {
		t.Run(preset, func(t *testing.T) {
			info, err := seeder.UploadMultiChunkInventory(t.Context(), fc.Raw(), seeder.Config{
				Target: seeder.TargetS3, Objects: objects, Preset: preset, Seed: 42, Logger: zerolog.Nop(),
			}, seeder.S3Config{Bucket: bucket, Prefix: "inv/", SrcBucket: "src"}, i+1, 42, stamp, 4)
			if err != nil {
				t.Fatalf("seed: %v", err)
			}
			manifestKey := strings.TrimPrefix(info.Path, "s3://"+bucket+"/")
			estimate, err := sizer.EstimatedIndexSize(t.Context(), bucket, manifestKey)
			if err != nil {
				t.Fatalf("EstimatedIndexSize: %v", err)
			}

			outDir, err := loader.New(t.TempDir(), fc).BuildWith(t.Context(),
				inventory.CacheKey{SourceBucket: "src", InventoryID: info.ID, Run: "run"}, info.Path, nil, nil)
			if err != nil {
				t.Fatalf("BuildWith: %v", err)
			}
			built, err := inventory.MeasureDir(t.Context(), outDir)
			if err != nil {
				t.Fatalf("MeasureDir: %v", err)
			}

			t.Logf("index %d B, estimate %d B (%.2fx)", built, estimate, float64(estimate)/float64(built))
			if estimate < built {
				t.Errorf("estimate %d B is under the built index's %d B", estimate, built)
			}
			if 2*estimate > 5*built {
				t.Errorf("estimate %d B is over 2.5x the built index's %d B", estimate, built)
			}
		})
	}
}
