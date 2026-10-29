//go:build integration

package seeder_test

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
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

func newLocalS3Client(t *testing.T) *s3.Client {
	t.Helper()
	if os.Getenv(s3fetch.EnvEndpointURL) == "" {
		t.Fatal("AWS_ENDPOINT_URL_S3 not set — run `make test`")
	}
	c, err := seeder.NewS3ClientForTest(t.Context())
	if err != nil {
		t.Fatalf("newS3Client: %v", err)
	}

	return c
}

func TestUploadInventory_RoundTrip(t *testing.T) {
	client := newLocalS3Client(t)
	bucket := s3test.Bucket(t, client)
	srcBucket := "synthetic-prod"
	prefix := "inventory-data/"
	stamp := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	objects := 50

	info, err := seeder.UploadInventory(t.Context(), client, seeder.Config{
		Target: seeder.TargetS3, Objects: objects, Preset: "small", Seed: 7,
		Logger: zerolog.Nop(),
	}, seeder.S3Config{
		Bucket: bucket, Prefix: prefix, SrcBucket: srcBucket,
	}, 1, 7, stamp)
	if err != nil {
		t.Fatalf("UploadInventory: %v", err)
	}
	if info.ID != "inv-001" {
		t.Errorf("InventoryInfo.ID = %q, want inv-001", info.ID)
	}
	wantPath := "s3://" + bucket + "/inventory-data/synthetic-prod/inv-001/2026-05-14T12-00Z/manifest.json"
	if info.Path != wantPath {
		t.Errorf("InventoryInfo.Path = %q, want %q", info.Path, wantPath)
	}

	manifestKey := "inventory-data/synthetic-prod/inv-001/2026-05-14T12-00Z/manifest.json"
	checksumKey := "inventory-data/synthetic-prod/inv-001/2026-05-14T12-00Z/manifest.checksum"
	for _, key := range []string{manifestKey, checksumKey} {
		_, err := client.HeadObject(t.Context(), &s3.HeadObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key),
		})
		if err != nil {
			t.Errorf("HeadObject %s: %v", key, err)
		}
	}

	manifest := getManifest(t, client, bucket, manifestKey)
	if manifest["sourceBucket"] != srcBucket {
		t.Errorf("manifest sourceBucket = %v, want %s", manifest["sourceBucket"], srcBucket)
	}
	if manifest["fileFormat"] != "CSV" {
		t.Errorf("manifest fileFormat = %v, want CSV", manifest["fileFormat"])
	}
	files, ok := manifest["files"].([]any)
	if !ok || len(files) != 1 {
		t.Fatalf("manifest files = %v, want one entry", manifest["files"])
	}
	dataEntry, ok := files[0].(map[string]any)
	if !ok {
		t.Fatalf("manifest file entry not a map: %v", files[0])
	}
	dataKey, _ := dataEntry["key"].(string)
	if got := getCSVGzObjectCount(t, client, bucket, dataKey); got != objects {
		t.Errorf("data file row count = %d, want %d", got, objects)
	}
}

func TestUploadInventory_DataFileURLPlaced(t *testing.T) {
	client := newLocalS3Client(t)
	bucket := s3test.Bucket(t, client)
	stamp := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	_, err := seeder.UploadInventory(t.Context(), client, seeder.Config{
		Target: seeder.TargetS3, Objects: 10, Preset: "small", Seed: 1,
		Logger: zerolog.Nop(),
	}, seeder.S3Config{Bucket: bucket, SrcBucket: "src"}, 1, 1, stamp)
	if err != nil {
		t.Fatalf("UploadInventory: %v", err)
	}

	pages := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("src/inv-001/data/"),
	})
	count := 0
	for pages.HasMorePages() {
		page, err := pages.NextPage(t.Context())
		if err != nil {
			t.Fatalf("list data dir: %v", err)
		}
		count += len(page.Contents)
	}
	if count != 1 {
		t.Errorf("data dir holds %d objects, want exactly 1", count)
	}
}

func TestUploadInventory_ParquetManifest(t *testing.T) {
	client := newLocalS3Client(t)
	bucket := s3test.Bucket(t, client)
	stamp := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	_, err := seeder.UploadInventory(t.Context(), client, seeder.Config{
		Target: seeder.TargetS3, Objects: 10, Preset: "small", Format: seeder.FormatParquet,
		Logger: zerolog.Nop(),
	}, seeder.S3Config{Bucket: bucket, SrcBucket: "src"}, 1, 1, stamp)
	if err != nil {
		t.Fatalf("UploadInventory: %v", err)
	}

	manifest := getManifest(t, client, bucket, "src/inv-001/2026-05-14T12-00Z/manifest.json")
	if manifest["fileFormat"] != "Parquet" {
		t.Errorf("manifest fileFormat = %v, want Parquet", manifest["fileFormat"])
	}
	if schema, _ := manifest["fileSchema"].(string); !strings.HasPrefix(schema, "message s3.inventory {") {
		t.Errorf("manifest fileSchema = %q, want a Parquet message", schema)
	}
	files, _ := manifest["files"].([]any)
	if len(files) != 1 {
		t.Fatalf("manifest files = %v, want one entry", manifest["files"])
	}
	entry, _ := files[0].(map[string]any)
	if key, _ := entry["key"].(string); !strings.HasSuffix(key, ".parquet") {
		t.Errorf("data file key = %q, want a .parquet key", key)
	}
}

// prefixTotals is one prefix's row in a built index.
type prefixTotals struct {
	tiers []format.TierBreakdown
	stats indexread.Stats
}

// buildTotals seeds cfg's inventory in four data files and returns
// every prefix's totals from the index the pipeline builds from it.
func buildTotals(t *testing.T, fc *s3fetch.Client, bucket string, cfg seeder.Config) map[string]prefixTotals {
	t.Helper()
	info, err := seeder.UploadMultiChunkInventory(t.Context(), fc.Raw(), cfg,
		seeder.S3Config{Bucket: bucket, Prefix: string(cfg.Format) + "/", SrcBucket: "src"},
		1, 11, time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC), 4)
	if err != nil {
		t.Fatalf("UploadMultiChunkInventory(%s): %v", cfg.Format, err)
	}
	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), info.Path, outDir); err != nil {
		t.Fatalf("pipeline.Run(%s): %v", cfg.Format, err)
	}
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("indexread.Open(%s): %v", cfg.Format, err)
	}
	defer idx.Close()
	totals := make(map[string]prefixTotals, idx.Count())
	for pos := range idx.Count() {
		prefix, err := idx.PrefixString(pos)
		if err != nil {
			t.Fatalf("PrefixString(%d): %v", pos, err)
		}
		totals[prefix] = prefixTotals{stats: idx.Stats(pos), tiers: idx.TierBreakdown(pos)}
	}

	return totals
}

// A Parquet and a CSV inventory of the same objects build the same
// index: every prefix's object count, bytes and tier breakdown.
func TestParquetInventory_BuildsSameIndexAsCSV(t *testing.T) {
	const objects = 20_000
	fc := s3test.FetchClient(t)
	bucket := s3test.Bucket(t, fc.Raw())
	cfg := seeder.Config{Target: seeder.TargetS3, Objects: objects, Preset: "realistic", Logger: zerolog.Nop()}

	cfg.Format = seeder.FormatCSV
	want := buildTotals(t, fc, bucket, cfg)
	cfg.Format = seeder.FormatParquet
	got := buildTotals(t, fc, bucket, cfg)

	if root := want[""]; root.stats.ObjectCount != objects || len(root.tiers) < 2 {
		t.Fatalf("csv root = %+v, want %d objects over several tiers", root, objects)
	}
	if len(got) != len(want) {
		t.Errorf("parquet index has %d prefixes, csv %d", len(got), len(want))
	}
	for prefix, w := range want {
		g, ok := got[prefix]
		if !ok {
			t.Errorf("prefix %q missing from the parquet index", prefix)

			continue
		}
		if g.stats != w.stats || !slices.Equal(g.tiers, w.tiers) {
			t.Errorf("prefix %q: parquet %+v, csv %+v", prefix, g, w)
		}
	}
}

func getManifest(t *testing.T, c *s3.Client, bucket, key string) map[string]any {
	t.Helper()
	resp, err := c.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject manifest: %v", err)
	}
	defer resp.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}

	return m
}

func getCSVGzObjectCount(t *testing.T, c *s3.Client, bucket, key string) int {
	t.Helper()
	resp, err := c.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject data: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read data body: %v", err)
	}
	gr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer gr.Close()
	rows, err := csv.NewReader(gr).ReadAll()
	if err != nil {
		t.Fatalf("csv.ReadAll: %v", err)
	}

	return len(rows)
}
