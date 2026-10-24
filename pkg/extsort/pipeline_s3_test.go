//go:build integration

package extsort_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/rs/zerolog"
)

type seeded struct {
	client   *s3fetch.Client
	manifest string // s3:// URI
}

// seedManifest uploads a small synthetic inventory.
func seedManifest(t *testing.T) seeded {
	t.Helper()
	fc := s3test.FetchClient(t)
	bucket := s3test.Bucket(t, fc.Raw())
	info, err := seeder.UploadInventory(t.Context(), fc.Raw(), seeder.Config{
		Target:  seeder.TargetS3,
		Objects: 500,
		Preset:  "small",
		Seed:    7,
		Logger:  zerolog.Nop(),
	}, seeder.S3Config{Bucket: bucket, Prefix: "inv/", SrcBucket: "src"}, 1, 7, time.Now().UTC().Truncate(time.Minute))
	if err != nil {
		t.Fatalf("UploadInventory: %v", err)
	}

	return seeded{client: fc, manifest: info.Path}
}

func TestPipelineRun_ReplacesPreviousIndex(t *testing.T) {
	s := seedManifest(t)
	fc, manifest := s.client, s.manifest
	outDir := filepath.Join(t.TempDir(), "index")

	for range 2 {
		if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, outDir); err != nil {
			t.Fatalf("Run: %v", err)
		}
	}
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("open rebuilt index: %v", err)
	}
	defer idx.Close()
	if idx.Count() == 0 {
		t.Error("rebuilt index is empty")
	}
	entries, err := os.ReadDir(filepath.Dir(outDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("staging or replaced directories left behind: %v", entries)
	}
}

func TestPipelineRun_FailedFinalSpillFailsTheBuild(t *testing.T) {
	s := seedManifest(t)
	fc, manifest := s.client, s.manifest
	outDir := filepath.Join(t.TempDir(), "index")
	// A small inventory spills only once, at the end of each worker. A
	// temp "dir" that is a regular file makes that spill fail for any
	// user, root included.
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := extsort.DefaultConfig()
	cfg.TempDir = notADir

	if _, err := extsort.NewPipeline(cfg, fc).Run(t.Context(), manifest, outDir); err == nil {
		t.Fatal("Run succeeded although every run file failed to write")
	}
	if _, err := os.Stat(outDir); !os.IsNotExist(err) {
		t.Errorf("failed build left something at outDir: %v", err)
	}
}

func TestPipelineRun_FailureKeepsPreviousIndex(t *testing.T) {
	s := seedManifest(t)
	fc, manifest := s.client, s.manifest
	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, outDir); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	before, err := os.ReadFile(filepath.Join(outDir, format.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}

	missing := manifest[:len(manifest)-len("manifest.json")] + "missing.json"
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), missing, outDir); err == nil {
		t.Fatal("Run succeeded with a missing manifest")
	}

	after, err := os.ReadFile(filepath.Join(outDir, format.ManifestFile))
	if err != nil {
		t.Fatalf("previous index damaged by a failed build: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Error("previous index changed by a failed build")
	}
	entries, err := os.ReadDir(filepath.Dir(outDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("staging directory left behind: %v", entries)
	}
}
