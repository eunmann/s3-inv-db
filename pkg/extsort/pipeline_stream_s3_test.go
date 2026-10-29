//go:build integration

package extsort_test

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/s3inventory"
)

var errDropped = errors.New("connection reset by peer")

// csvChunk is one data file of a test inventory.
type csvChunk struct {
	key       string
	rows      int
	gzipped   bool
	multipart bool // uploaded in 5 MiB parts
}

// uploadStreamInventory writes a CSV inventory of chunks under inv/ and
// returns its manifest URI.
func uploadStreamInventory(t *testing.T, raw *s3.Client, bucket string, chunks []csvChunk) string {
	t.Helper()
	files := make([]map[string]any, 0, len(chunks))
	for c, chunk := range chunks {
		var buf bytes.Buffer
		var out io.Writer = &buf
		gz := gzip.NewWriter(&buf)
		if chunk.gzipped {
			out = gz
		}
		w := csv.NewWriter(out)
		for i := range chunk.rows {
			key := fmt.Sprintf("dir%02d/sub%d/obj-%02d-%06d.bin", (c+i)%11, i%5, c, i)
			if err := w.Write([]string{"src", key, strconv.Itoa(c*1_000_000 + i + 1)}); err != nil {
				t.Fatal(err)
			}
		}
		w.Flush()
		if err := w.Error(); err != nil {
			t.Fatal(err)
		}
		if chunk.gzipped {
			if err := gz.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if chunk.multipart {
			putMultipartObject(t, raw, bucket, chunk.key, buf.Bytes())
		} else if _, err := raw.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(chunk.key), Body: bytes.NewReader(buf.Bytes()),
		}); err != nil {
			t.Fatal(err)
		}
		files = append(files, map[string]any{"key": chunk.key, "size": buf.Len()})
	}
	manifest, err := json.Marshal(map[string]any{
		"sourceBucket":      "src",
		"destinationBucket": "arn:aws:s3:::" + bucket,
		"version":           "2016-11-30",
		"fileFormat":        "CSV",
		"fileSchema":        "Bucket, Key, Size",
		"files":             files,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("inv/manifest.json"), Body: bytes.NewReader(manifest),
	}); err != nil {
		t.Fatal(err)
	}

	return "s3://" + bucket + "/inv/manifest.json"
}

func putMultipartObject(t *testing.T, raw *s3.Client, bucket, key string, body []byte) {
	t.Helper()
	ctx := t.Context()
	mp, err := raw.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	const partSize = 5 << 20
	if len(body) <= partSize {
		t.Fatalf("%s is %d bytes, too small for more than one part", key, len(body))
	}
	var parts []types.CompletedPart
	for i := 0; i*partSize < len(body); i++ {
		num := aws.Int32(int32(i + 1))
		out, err := raw.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: mp.UploadId, PartNumber: num,
			Body: bytes.NewReader(body[i*partSize : min((i+1)*partSize, len(body))]),
		})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, types.CompletedPart{ETag: out.ETag, PartNumber: num})
	}
	if _, err := raw.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: mp.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		t.Fatal(err)
	}
}

type prefixTotals struct{ objects, bytes uint64 }

// downloadedTotals counts every prefix's objects and bytes from the
// chunks as the file path reads them: downloaded whole, then parsed.
func downloadedTotals(t *testing.T, fc *s3fetch.Client, bucket string, chunks []csvChunk) map[string]prefixTotals {
	t.Helper()
	want := map[string]prefixTotals{}
	cols := s3inventory.CSVReaderConfig{KeyCol: s3inventory.ColumnAt(1), SizeCol: s3inventory.ColumnAt(2)}
	for _, chunk := range chunks {
		obj, _, err := fc.DownloadObject(t.Context(), bucket, chunk.key)
		if err != nil {
			t.Fatal(err)
		}
		r, err := s3inventory.NewCSVReaderFromStream(obj, cols)
		if err != nil {
			t.Fatal(err)
		}
		for {
			row, err := r.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, prefix := range ancestorPrefixes(strings.Clone(row.Key)) {
				w := want[prefix]
				w.objects++
				w.bytes += row.Size
				want[prefix] = w
			}
		}
		_ = r.Close()
		_ = obj.Close()
	}

	return want
}

// A CSV inventory streams into exactly the index the downloaded files
// give: gzip and plain chunks, single-part and multipart, with every
// GET's body dropped once mid-way and resumed. Its chunks never touch
// scratch.
func TestPipelineRun_CSVStreamsExactly(t *testing.T) {
	raw := s3test.RawClient(t)
	bucket := s3test.Bucket(t, raw)
	chunks := []csvChunk{
		{key: "inv/data/big.csv", rows: 250_000, multipart: true},
		{key: "inv/data/a.csv.gz", rows: 20_000, gzipped: true},
		{key: "inv/data/b.csv", rows: 5_000},
		{key: "inv/data/c.csv.gz", rows: 1, gzipped: true},
	}
	manifest := uploadStreamInventory(t, raw, bucket, chunks)
	want := downloadedTotals(t, s3test.FetchClient(t), bucket, chunks)

	var mu sync.Mutex
	seen := map[string]bool{}
	var drops atomic.Int64
	fc := s3test.FaultyFetchClient(t, func(req *http.Request, body io.ReadCloser) io.ReadCloser {
		if !strings.Contains(req.URL.Path, "/data/") {
			return body
		}
		get := req.URL.Path + "?" + req.URL.Query().Get("partNumber")
		mu.Lock()
		first := !seen[get]
		seen[get] = true
		mu.Unlock()
		if !first {
			return body
		}
		drops.Add(1)

		return s3test.FailAfter(body, 1000, errDropped)
	})

	scratch := t.TempDir()
	var downloads atomic.Int64
	stop := make(chan struct{})
	var sampler sync.WaitGroup
	sampler.Go(func() {
		for {
			if matches, _ := filepath.Glob(filepath.Join(scratch, "s3download-*.tmp")); len(matches) > 0 {
				downloads.Add(int64(len(matches)))
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
	_, runErr := extsort.NewPipeline(cfg, fc).Run(t.Context(), manifest, outDir)
	close(stop)
	sampler.Wait()
	if runErr != nil {
		t.Fatalf("Run: %v", runErr)
	}
	if n := drops.Load(); n < int64(len(chunks)+1) {
		t.Errorf("%d GETs dropped, want one per chunk and per part of the multipart one", n)
	}
	if n := downloads.Load(); n != 0 {
		t.Errorf("a CSV chunk was downloaded to scratch (%d samples)", n)
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
		if !ok || st.ObjectCount != w.objects || st.TotalBytes != w.bytes {
			t.Errorf("prefix %q = %+v (%v), want %d objects / %d bytes", prefix, st, ok, w.objects, w.bytes)
		}
	}
}

// A chunk whose body ends early on every GET fails the build: a plain
// CSV cut at a line end parses cleanly, so only the stream's size check
// stands between it and an index missing rows.
func TestPipelineRun_CSVTruncatedChunkFails(t *testing.T) {
	raw := s3test.RawClient(t)
	bucket := s3test.Bucket(t, raw)
	manifest := uploadStreamInventory(t, raw, bucket, []csvChunk{{key: "inv/data/a.csv", rows: 2_000}})
	fc := s3test.FaultyFetchClient(t, func(req *http.Request, body io.ReadCloser) io.ReadCloser {
		if !strings.Contains(req.URL.Path, "/data/") {
			return body
		}

		return s3test.FailAfter(body, 10_000, io.EOF)
	})

	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, outDir); err == nil {
		t.Fatal("Run succeeded on a chunk that ends early")
	}
	if _, err := os.Stat(outDir); !os.IsNotExist(err) {
		t.Errorf("failed build left something at outDir: %v", err)
	}
}
