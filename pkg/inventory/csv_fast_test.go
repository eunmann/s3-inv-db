package inventory

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"
	"testing"
)

// TestFastCSVMatchesStdlib feeds the same bytes through both readers and
// checks every Row matches. This is the correctness pin for I9 — if it
// passes we trust the hand-rolled parser to substitute encoding/csv on
// AWS-shaped inputs.
func TestFastCSVMatchesStdlib(t *testing.T) {
	csv := strings.Join([]string{
		// AWS-style 9-column inventory CSV (bucket, key, size, last_modified,
		// etag, storage_class, multipart, replication, encryption).
		"my-bucket,a/b/c.parquet,4096,2024-01-01T00:00:00.000Z,abc,STANDARD,false,REPLICA,SSE-KMS",
		"my-bucket,a/b/d.parquet,8192,2024-01-01T00:00:00.000Z,def,GLACIER,false,REPLICA,SSE-KMS",
		"my-bucket,x/y/z.txt,12,2024-01-01T00:00:00.000Z,xyz,INTELLIGENT_TIERING,false,REPLICA,SSE-KMS",
		"my-bucket,empty-key-test,0,2024-01-01T00:00:00.000Z,000,STANDARD,false,REPLICA,SSE-KMS",
	}, "\n") + "\n"

	cfg := CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 5, AccessTierCol: -1}

	std := NewCSVInventoryReader(strings.NewReader(csv), cfg)
	defer std.Close()
	fast := NewCSVInventoryReaderFast(strings.NewReader(csv), cfg)
	defer fast.Close()

	for i := 0; ; i++ {
		stdRow, stdErr := std.Next()
		fastRow, fastErr := fast.Next()

		if (stdErr == nil) != (fastErr == nil) {
			t.Fatalf("row %d: stdErr=%v fastErr=%v", i, stdErr, fastErr)
		}
		if stdErr == io.EOF {
			return
		}
		if stdErr != nil {
			t.Fatalf("row %d: unexpected err: %v", i, stdErr)
		}
		if stdRow.Key != fastRow.Key {
			t.Errorf("row %d Key: std=%q fast=%q", i, stdRow.Key, fastRow.Key)
		}
		if stdRow.Size != fastRow.Size {
			t.Errorf("row %d Size: std=%d fast=%d", i, stdRow.Size, fastRow.Size)
		}
		if stdRow.StorageClass != fastRow.StorageClass {
			t.Errorf("row %d Storage: std=%q fast=%q", i, stdRow.StorageClass, fastRow.StorageClass)
		}
	}
}

// TestFastCSVRejectsQuotes pins the safety net: if AWS ever quotes a
// field, the fast parser MUST error so the caller can fall back.
func TestFastCSVRejectsQuotes(t *testing.T) {
	csv := `my-bucket,"a/quoted,key",100,2024-01-01,abc,STANDARD,false,REPLICA,SSE-KMS` + "\n"
	cfg := CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 5, AccessTierCol: -1}
	fast := NewCSVInventoryReaderFast(strings.NewReader(csv), cfg)
	defer fast.Close()
	if _, err := fast.Next(); err == nil {
		t.Fatal("expected error on quoted row, got nil")
	}
}

// TestFastCSVGzipStream exercises the same path the production pipeline
// uses (gzipped CSV from a stream).
func TestFastCSVGzipStream(t *testing.T) {
	var raw bytes.Buffer
	for i := range 1000 {
		fmt.Fprintf(&raw, "my-bucket,key-%05d,%d,2024-01-01,etag,STANDARD,false,REPLICA,SSE-KMS\n",
			i, i*1024)
	}
	var gz bytes.Buffer
	gzw := gzip.NewWriter(&gz)
	gzw.Write(raw.Bytes())
	gzw.Close()

	cfg := CSVReaderConfig{KeyCol: 1, SizeCol: 2, StorageCol: 5, AccessTierCol: -1}
	rc := io.NopCloser(bytes.NewReader(gz.Bytes()))
	r, err := NewCSVInventoryReaderFastFromStream(rc, "data.csv.gz", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	count := 0
	for {
		row, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(row.Key, "key-") {
			t.Fatalf("bad key %q", row.Key)
		}
		count++
	}
	if count != 1000 {
		t.Errorf("got %d rows, want 1000", count)
	}
}
