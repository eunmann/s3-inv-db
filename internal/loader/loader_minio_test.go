package loader_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/internal/miniotest"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/rs/zerolog"
)

func TestBuildWith_BuildsIndexFromSeededManifest(t *testing.T) {
	fc := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, fc.Raw())

	srcBucket := "synthetic-prod"
	prefix := "inventory-data/"
	stamp := time.Now().UTC().Truncate(time.Minute)

	info, err := seeder.UploadInventory(context.Background(), fc.Raw(), seeder.Config{
		Target:  seeder.TargetS3,
		Objects: 200,
		Preset:  "small",
		Seed:    42,
		Logger:  zerolog.Nop(),
	}, seeder.S3Config{
		Bucket:    bucket,
		Prefix:    prefix,
		SrcBucket: srcBucket,
	}, 1, 42, stamp)
	if err != nil {
		t.Fatalf("seeder.UploadInventory: %v", err)
	}

	cacheRoot := t.TempDir()
	l := loader.New(cacheRoot, fc)

	run := stamp.Format("2006-01-02T15-04Z")
	var stages []string
	outDir, err := l.BuildWith(context.Background(), srcBucket, info.ID, run, info.Path,
		func(stage string, _, _ int64) { stages = append(stages, stage) },
	)
	if err != nil {
		t.Fatalf("BuildWith: %v", err)
	}
	if outDir != l.CacheDirFor(srcBucket, info.ID, run) {
		t.Errorf("outDir = %q, want %q", outDir, l.CacheDirFor(srcBucket, info.ID, run))
	}
	for _, want := range []string{"preparing", "done"} {
		found := slices.Contains(stages, want)
		if !found {
			t.Errorf("stages = %v, missing %q", stages, want)
		}
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("open built index: %v", err)
	}
	defer idx.Close()
	if idx.Count() == 0 {
		t.Error("index has zero prefixes")
	}
	pos, ok := idx.Lookup("")
	if !ok {
		t.Fatal("root prefix missing")
	}
	stats := idx.Stats(pos)
	if stats.ObjectCount == 0 {
		t.Error("root prefix has zero objects")
	}
}

func TestBuild_FailsOnMissingManifest(t *testing.T) {
	fc := miniotest.FetchClient(t)
	bucket := miniotest.Bucket(t, fc.Raw())

	l := loader.New(t.TempDir(), fc)
	bogus := fmt.Sprintf("s3://%s/does/not/exist/manifest.json", bucket)
	_, err := l.Build(context.Background(), "src", "inv", "run", bogus)
	if err == nil {
		t.Fatal("Build with missing manifest returned nil error")
	}
	if !strings.Contains(err.Error(), "pipeline") {
		t.Logf("build error (informational): %v", err)
	}
}
