//go:build integration

package extsort_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/parquet-go/parquet-go"
)

// awsParquetSchema is the fileSchema AWS writes in a Parquet inventory's
// manifest: a Parquet message, not a column list.
const awsParquetSchema = "message s3.inventory { required binary bucket (UTF8); required binary key (UTF8); " +
	"optional binary version_id (UTF8); optional boolean is_latest; optional boolean is_delete_marker; " +
	"optional int64 size; optional int64 last_modified_date (TIMESTAMP_MILLIS); optional binary e_tag (UTF8); " +
	"optional binary storage_class (UTF8);}"

type parquetInventoryRow struct {
	Bucket       string `parquet:"bucket"`
	Key          string `parquet:"key"`
	Size         int64  `parquet:"size"`
	StorageClass string `parquet:"storage_class"`
}

// A multi-chunk Parquet inventory behind an AWS-shaped manifest builds,
// every prefix's totals equal a direct count of the rows, and scratch
// never holds more than two downloaded chunks per ingest worker: each
// chunk leaves scratch once parsed.
func TestPipelineRun_ParquetInventory(t *testing.T) {
	fc := s3test.FetchClient(t)
	bucket := s3test.Bucket(t, fc.Raw())
	put := func(key string, body []byte) {
		t.Helper()
		if _, err := fc.Raw().PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	numChunks := 4 * runtime.NumCPU()
	const rowsPerChunk = 50
	type totals struct{ objects, bytes uint64 }
	want := map[string]totals{}
	files := make([]map[string]any, 0, numChunks)
	for c := range numChunks {
		rows := make([]parquetInventoryRow, rowsPerChunk)
		for i := range rows {
			key := fmt.Sprintf("dir%02d/sub%d/obj-%03d-%03d.bin", c%7, i%3, c, i)
			rows[i] = parquetInventoryRow{Bucket: "src", Key: key, Size: int64(c*1000 + i + 1), StorageClass: "STANDARD"}
			for _, prefix := range ancestorPrefixes(key) {
				w := want[prefix]
				w.objects++
				w.bytes += uint64(rows[i].Size)
				want[prefix] = w
			}
		}
		var buf bytes.Buffer
		if err := parquet.Write(&buf, rows); err != nil {
			t.Fatalf("write parquet: %v", err)
		}
		key := fmt.Sprintf("inv/data/part-%03d.parquet", c)
		put(key, buf.Bytes())
		files = append(files, map[string]any{"key": key, "size": buf.Len()})
	}
	manifest, err := json.Marshal(map[string]any{
		"sourceBucket":      "src",
		"destinationBucket": "arn:aws:s3:::" + bucket,
		"version":           "2016-11-30",
		"fileFormat":        "Parquet",
		"fileSchema":        awsParquetSchema,
		"files":             files,
	})
	if err != nil {
		t.Fatal(err)
	}
	put("inv/manifest.json", manifest)

	scratch := t.TempDir()
	var maxChunks atomic.Int64
	stop := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for {
			matches, _ := filepath.Glob(filepath.Join(scratch, "s3download-*.tmp"))
			if n := int64(len(matches)); n > maxChunks.Load() {
				maxChunks.Store(n)
			}
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	})
	cfg := extsort.DefaultConfig()
	cfg.TempDir = scratch
	outDir := filepath.Join(t.TempDir(), "index")
	_, runErr := extsort.NewPipeline(cfg, fc).Run(t.Context(), "s3://"+bucket+"/inv/manifest.json", outDir)
	close(stop)
	sampler.Wait()
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}

	workers := min(runtime.GOMAXPROCS(0), numChunks)
	if got := maxChunks.Load(); got > int64(2*workers) {
		t.Errorf("scratch held %d downloaded chunks at once, want at most %d (two per worker)", got, 2*workers)
	}
	if left, _ := filepath.Glob(filepath.Join(scratch, "s3download-*.tmp")); len(left) != 0 {
		t.Errorf("%d downloaded chunks left in scratch after the build", len(left))
	}

	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if got := idx.Count(); got != uint64(len(want)) {
		t.Errorf("index has %d prefixes, want %d", got, len(want))
	}
	for prefix, w := range want {
		st, ok := idx.StatsForPrefix(prefix)
		if !ok {
			t.Errorf("prefix %q missing", prefix)

			continue
		}
		if st.ObjectCount != w.objects || st.TotalBytes != w.bytes {
			t.Errorf("prefix %q = %d objects / %d bytes, want %d / %d", prefix, st.ObjectCount, st.TotalBytes, w.objects, w.bytes)
		}
	}
}

// ancestorPrefixes returns "" and every directory prefix of key.
func ancestorPrefixes(key string) []string {
	prefixes := []string{""}
	for i := range len(key) {
		if key[i] == '/' {
			prefixes = append(prefixes, key[:i+1])
		}
	}

	return prefixes
}
