//go:build integration

package extsort_test

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// uploadCSVInventory writes a one-file CSV inventory with the given
// schema and rows and returns its manifest URI.
func uploadCSVInventory(t *testing.T, fc *s3fetch.Client, schema string, rows [][]string) string {
	t.Helper()
	bucket := s3test.Bucket(t, fc.Raw())
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	w := csv.NewWriter(gz)
	if err := w.WriteAll(rows); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	put := func(key string, body []byte) {
		if _, err := fc.Raw().PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	put("inv/data/part-0.csv.gz", buf.Bytes())
	manifest, err := json.Marshal(map[string]any{
		"sourceBucket":      "src",
		"destinationBucket": "arn:aws:s3:::" + bucket,
		"version":           "2016-11-30",
		"fileFormat":        "CSV",
		"fileSchema":        schema,
		"files":             []map[string]any{{"key": "inv/data/part-0.csv.gz", "size": buf.Len()}},
	})
	if err != nil {
		t.Fatal(err)
	}
	put("inv/manifest.json", manifest)

	return "s3://" + bucket + "/inv/manifest.json"
}

// An all-versions CSV inventory: keys are URL-encoded, delete markers
// have no size and are not objects, noncurrent versions are stored bytes.
func TestPipelineRun_CSVEncodedKeysAndVersions(t *testing.T) {
	fc := s3test.FetchClient(t)
	manifest := uploadCSVInventory(t, fc,
		"Bucket, Key, VersionId, IsLatest, IsDeleteMarker, Size, StorageClass",
		[][]string{
			{"src", "my+folder/a%2Bb.txt", "v2", "true", "false", "100", "STANDARD"},
			{"src", "my+folder/a%2Bb.txt", "v1", "false", "false", "40", "STANDARD"},
			{"src", "my+folder/gone.txt", "v3", "true", "true", "", ""},
			{"src", "donn%C3%A9es/x.csv", "v1", "true", "false", "7", "GLACIER"},
		})
	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, outDir); err != nil {
		t.Fatalf("Run: %v", err)
	}
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	cases := []struct {
		prefix         string
		objects, bytes uint64
	}{
		{"my folder/", 2, 140}, // two stored versions; the delete marker is not counted
		{"données/", 1, 7},
		{"", 3, 147},
	}
	for _, c := range cases {
		st, ok := idx.StatsForPrefix(c.prefix)
		if !ok {
			t.Errorf("prefix %q missing", c.prefix)

			continue
		}
		if st.ObjectCount != c.objects || st.TotalBytes != c.bytes {
			t.Errorf("prefix %q = %d objects / %d bytes, want %d / %d", c.prefix, st.ObjectCount, st.TotalBytes, c.objects, c.bytes)
		}
	}
	if _, ok := idx.StatsForPrefix("my+folder/"); ok {
		t.Error("undecoded prefix my+folder/ present in the index")
	}
}

// A row that can't be read exactly fails the build (exact answers):
// here a size that doesn't parse. AllowAnomalies accepts it, indexed as 0.
func TestPipelineRun_FailsOnAnomalousRows(t *testing.T) {
	fc := s3test.FetchClient(t)
	manifest := uploadCSVInventory(t, fc, "Bucket, Key, Size",
		[][]string{
			{"src", "a/ok.txt", "100"},
			{"src", "a/bad.txt", "12x"},
		})

	_, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, filepath.Join(t.TempDir(), "index"))
	if !errors.Is(err, extsort.ErrInventoryAnomalies) {
		t.Fatalf("Run error = %v, want ErrInventoryAnomalies", err)
	}

	cfg := extsort.DefaultConfig()
	cfg.AllowAnomalies = true
	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(cfg, fc).Run(t.Context(), manifest, outDir); err != nil {
		t.Fatalf("Run with AllowAnomalies: %v", err)
	}
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if st, ok := idx.StatsForPrefix("a/"); !ok || st.ObjectCount != 2 || st.TotalBytes != 100 {
		t.Errorf("a/ = %+v, %v; want 2 objects, 100 bytes", st, ok)
	}
}

// A downloaded chunk whose size differs from its manifest entry (a
// truncated or replaced file) fails the build.
func TestPipelineRun_FailsOnChunkSizeMismatch(t *testing.T) {
	fc := s3test.FetchClient(t)
	manifest := uploadCSVInventory(t, fc, "Bucket, Key, Size", [][]string{{"src", "a/ok.txt", "100"}})
	bucket, key := splitS3URI(t, manifest)
	out, err := fc.Raw().GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.NewDecoder(out.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	_ = out.Body.Close()
	file := m["files"].([]any)[0].(map[string]any) //nolint:forcetypeassert // our own fixture
	file["size"] = file["size"].(float64) + 1      //nolint:forcetypeassert // our own fixture
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fc.Raw().PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
	}); err != nil {
		t.Fatal(err)
	}

	_, err = extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, filepath.Join(t.TempDir(), "index"))
	if !errors.Is(err, extsort.ErrChunkSizeMismatch) {
		t.Fatalf("Run error = %v, want ErrChunkSizeMismatch", err)
	}
}

// splitS3URI splits "s3://bucket/key" into bucket and key.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func splitS3URI(t *testing.T, uri string) (string, string) {
	t.Helper()
	rest, ok := strings.CutPrefix(uri, "s3://")
	bucket, key, found := strings.Cut(rest, "/")
	if !ok || !found {
		t.Fatalf("not an s3 URI: %q", uri)
	}

	return bucket, key
}

// A build keeps its scratch (downloads, run files, MPHF scratch) beside
// the output, never in os.TempDir: TMPDIR here is read-only, and the
// output's parent holds only the index afterwards.
func TestPipelineRun_ScratchStaysBesideTheIndex(t *testing.T) {
	s := seedManifest(t)
	readOnly := t.TempDir()
	if err := os.Chmod(readOnly, 0o500); err != nil { //nolint:gosec // the test needs a directory nothing can write to
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(readOnly, 0o700) }) //nolint:gosec // restore so t.TempDir can clean up
	t.Setenv("TMPDIR", readOnly)

	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), s.client).Run(t.Context(), s.manifest, outDir); err != nil {
		t.Fatalf("Run with a read-only TMPDIR: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(outDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "index" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("output parent holds %v, want only the index", names)
	}
}
