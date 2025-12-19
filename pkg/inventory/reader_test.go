package inventory

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecordType(t *testing.T) {
	r := Record{Key: "test/key.txt", Size: 1024}
	if r.Key != "test/key.txt" {
		t.Errorf("Key = %s, want test/key.txt", r.Key)
	}
	if r.Size != 1024 {
		t.Errorf("Size = %d, want 1024", r.Size)
	}
}

func TestNewReader(t *testing.T) {
	csv := "a/b/c.txt,100\nd/e.txt,200\n"
	r := NewReader(strings.NewReader(csv), 0, 1)

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "a/b/c.txt" || rec.Size != 100 {
		t.Errorf("got %+v, want {Key:a/b/c.txt Size:100}", rec)
	}

	rec, err = r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "d/e.txt" || rec.Size != 200 {
		t.Errorf("got %+v, want {Key:d/e.txt Size:200}", rec)
	}

	_, err = r.Read()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestOpenFile_CSV(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")

	content := "Bucket,Key,Size,LastModified\nmy-bucket,a/b/c.txt,1024,2024-01-01\nmy-bucket,d/e.txt,2048,2024-01-02\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	r, err := OpenFile(csvPath)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer r.Close()

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "a/b/c.txt" || rec.Size != 1024 {
		t.Errorf("got %+v, want {Key:a/b/c.txt Size:1024}", rec)
	}

	rec, err = r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "d/e.txt" || rec.Size != 2048 {
		t.Errorf("got %+v, want {Key:d/e.txt Size:2048}", rec)
	}

	_, err = r.Read()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestOpenFile_CSVGZ(t *testing.T) {
	dir := t.TempDir()
	gzPath := filepath.Join(dir, "test.csv.gz")

	content := "Key,Size\na/b.txt,100\nc/d.txt,200\n"

	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	_, _ = gzw.Write([]byte(content))
	gzw.Close()

	if err := os.WriteFile(gzPath, buf.Bytes(), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	r, err := OpenFile(gzPath)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer r.Close()

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "a/b.txt" || rec.Size != 100 {
		t.Errorf("got %+v, want {Key:a/b.txt Size:100}", rec)
	}

	rec, err = r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "c/d.txt" || rec.Size != 200 {
		t.Errorf("got %+v, want {Key:c/d.txt Size:200}", rec)
	}
}

func TestOpenFile_MissingKeyColumn(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")

	content := "Bucket,Size\nmy-bucket,1024\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := OpenFile(csvPath)
	if !errors.Is(err, ErrNoKeyColumn) {
		t.Errorf("expected ErrNoKeyColumn, got %v", err)
	}
}

func TestOpenFile_MissingSizeColumn(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")

	content := "Bucket,Key\nmy-bucket,a/b.txt\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := OpenFile(csvPath)
	if !errors.Is(err, ErrNoSizeColumn) {
		t.Errorf("expected ErrNoSizeColumn, got %v", err)
	}
}

func TestOpenFile_CaseInsensitiveHeaders(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")

	content := "BUCKET,KEY,SIZE\nmy-bucket,test.txt,512\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	r, err := OpenFile(csvPath)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer r.Close()

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "test.txt" || rec.Size != 512 {
		t.Errorf("got %+v, want {Key:test.txt Size:512}", rec)
	}
}

func TestOpenFile_WhitespaceInHeaders(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")

	content := "  Key  ,  Size  \ntest.txt,256\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	r, err := OpenFile(csvPath)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	defer r.Close()

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "test.txt" {
		t.Errorf("Key = %q, want test.txt", rec.Key)
	}
}

func TestRead_InvalidSize(t *testing.T) {
	csv := "abc/def.txt,not-a-number\n"
	r := NewReader(strings.NewReader(csv), 0, 1)

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	// Invalid size should be treated as 0
	if rec.Key != "abc/def.txt" || rec.Size != 0 {
		t.Errorf("got %+v, want {Key:abc/def.txt Size:0}", rec)
	}
}

func TestRead_EmptySize(t *testing.T) {
	csv := "abc/def.txt,\n"
	r := NewReader(strings.NewReader(csv), 0, 1)

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "abc/def.txt" || rec.Size != 0 {
		t.Errorf("got %+v, want {Key:abc/def.txt Size:0}", rec)
	}
}

func TestRead_EmptyKey(t *testing.T) {
	csv := ",100\nabc.txt,200\n"
	r := NewReader(strings.NewReader(csv), 0, 1)

	// Should skip empty key and return abc.txt
	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "abc.txt" {
		t.Errorf("Key = %q, want abc.txt", rec.Key)
	}
}

func TestRead_InsufficientColumns(t *testing.T) {
	csv := "only-one-column\nabc.txt,200\n"
	r := NewReader(strings.NewReader(csv), 0, 1)

	// Should skip row with insufficient columns
	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "abc.txt" {
		t.Errorf("Key = %q, want abc.txt", rec.Key)
	}
}

func TestRead_LargeKey(t *testing.T) {
	// S3 keys can be up to 1024 bytes
	longKey := strings.Repeat("a/", 500) + "file.txt"
	csv := longKey + ",1024\n"
	r := NewReader(strings.NewReader(csv), 0, 1)

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != longKey {
		t.Errorf("Key length = %d, want %d", len(rec.Key), len(longKey))
	}
}

func TestRead_UnicodeKey(t *testing.T) {
	csv := "日本語/ファイル.txt,100\n"
	r := NewReader(strings.NewReader(csv), 0, 1)

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "日本語/ファイル.txt" {
		t.Errorf("Key = %q, want 日本語/ファイル.txt", rec.Key)
	}
}

func TestRead_KeyWithQuotes(t *testing.T) {
	csv := `"file with ""quotes"".txt",100` + "\n"
	r := NewReader(strings.NewReader(csv), 0, 1)

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	expected := `file with "quotes".txt`
	if rec.Key != expected {
		t.Errorf("Key = %q, want %q", rec.Key, expected)
	}
}

func TestRead_KeyWithCommas(t *testing.T) {
	csv := `"file,with,commas.txt",100` + "\n"
	r := NewReader(strings.NewReader(csv), 0, 1)

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "file,with,commas.txt" {
		t.Errorf("Key = %q, want file,with,commas.txt", rec.Key)
	}
}

func TestRead_FolderMarker(t *testing.T) {
	csv := "folder/,0\nfolder/file.txt,100\n"
	r := NewReader(strings.NewReader(csv), 0, 1)

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "folder/" {
		t.Errorf("Key = %q, want folder/", rec.Key)
	}

	rec, err = r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "folder/file.txt" {
		t.Errorf("Key = %q, want folder/file.txt", rec.Key)
	}
}

func TestOpenFile_NotFound(t *testing.T) {
	_, err := OpenFile("/nonexistent/path/file.csv")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestOpenFile_InvalidGzip(t *testing.T) {
	dir := t.TempDir()
	gzPath := filepath.Join(dir, "invalid.csv.gz")

	// Write non-gzip content with .gz extension
	if err := os.WriteFile(gzPath, []byte("not gzip content"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := OpenFile(gzPath)
	if err == nil {
		t.Error("expected error for invalid gzip file")
	}
}

func TestClose_NilClosers(t *testing.T) {
	r := &CSVReader{}
	err := r.Close()
	if err != nil {
		t.Errorf("Close on empty reader failed: %v", err)
	}
}

func TestOpenFileWithSchema(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")

	// AWS S3 inventory CSV files have no header
	content := "my-bucket,a/b/c.txt,1024,2024-01-01\nmy-bucket,d/e.txt,2048,2024-01-02\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Key is column 1, Size is column 2
	r, err := OpenFileWithSchema(csvPath, 1, 2)
	if err != nil {
		t.Fatalf("OpenFileWithSchema failed: %v", err)
	}
	defer r.Close()

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "a/b/c.txt" || rec.Size != 1024 {
		t.Errorf("got %+v, want {Key:a/b/c.txt Size:1024}", rec)
	}

	rec, err = r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "d/e.txt" || rec.Size != 2048 {
		t.Errorf("got %+v, want {Key:d/e.txt Size:2048}", rec)
	}

	_, err = r.Read()
	if !errors.Is(err, io.EOF) {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestOpenFileWithSchema_CSVGZ(t *testing.T) {
	dir := t.TempDir()
	gzPath := filepath.Join(dir, "test.csv.gz")

	// AWS S3 inventory CSV files have no header
	content := "my-bucket,a/b.txt,100\nmy-bucket,c/d.txt,200\n"

	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	_, _ = gzw.Write([]byte(content))
	gzw.Close()

	if err := os.WriteFile(gzPath, buf.Bytes(), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Key is column 1, Size is column 2
	r, err := OpenFileWithSchema(gzPath, 1, 2)
	if err != nil {
		t.Fatalf("OpenFileWithSchema failed: %v", err)
	}
	defer r.Close()

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "a/b.txt" || rec.Size != 100 {
		t.Errorf("got %+v, want {Key:a/b.txt Size:100}", rec)
	}
}

func TestOpenFileWithOptions_TrackTiers(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")

	content := "Key,Size,StorageClass,IntelligentTieringAccessTier\n" +
		"file1.txt,100,STANDARD,\n" +
		"file2.txt,200,GLACIER,\n" +
		"file3.txt,300,INTELLIGENT_TIERING,FREQUENT_ACCESS\n" +
		"file4.txt,400,INTELLIGENT_TIERING,ARCHIVE_ACCESS\n"

	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	r, err := OpenFileWithOptions(csvPath, OpenOptions{TrackTiers: true})
	if err != nil {
		t.Fatalf("OpenFileWithOptions failed: %v", err)
	}
	defer r.Close()

	// file1.txt - STANDARD
	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "file1.txt" || rec.TierID != 0 { // Standard = 0
		t.Errorf("file1: got TierID=%d, want 0 (Standard)", rec.TierID)
	}

	// file2.txt - GLACIER
	rec, err = r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "file2.txt" || rec.TierID != 4 { // GlacierFR = 4
		t.Errorf("file2: got TierID=%d, want 4 (GlacierFR)", rec.TierID)
	}

	// file3.txt - IT FREQUENT
	rec, err = r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "file3.txt" || rec.TierID != 7 { // ITFrequent = 7
		t.Errorf("file3: got TierID=%d, want 7 (ITFrequent)", rec.TierID)
	}

	// file4.txt - IT ARCHIVE
	rec, err = r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.Key != "file4.txt" || rec.TierID != 10 { // ITArchive = 10
		t.Errorf("file4: got TierID=%d, want 10 (ITArchive)", rec.TierID)
	}
}

func TestOpenFileWithOptions_NoTrackTiers(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")

	content := "Key,Size,StorageClass\nfile1.txt,100,GLACIER\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Without TrackTiers, TierID should be 0 (default)
	r, err := OpenFileWithOptions(csvPath, OpenOptions{TrackTiers: false})
	if err != nil {
		t.Fatalf("OpenFileWithOptions failed: %v", err)
	}
	defer r.Close()

	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.TierID != 0 {
		t.Errorf("without tracking, got TierID=%d, want 0", rec.TierID)
	}
}

func TestOpenFileWithSchemaOptions_TrackTiers(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")

	// Headerless CSV with columns: Bucket, Key, Size, StorageClass, ITAccessTier
	content := "my-bucket,file1.txt,100,STANDARD_IA,\nmy-bucket,file2.txt,200,INTELLIGENT_TIERING,INFREQUENT_ACCESS\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	r, err := OpenFileWithSchemaOptions(csvPath, SchemaOptions{
		KeyCol:        1,
		SizeCol:       2,
		StorageCol:    3,
		AccessTierCol: 4,
		TrackTiers:    true,
	})
	if err != nil {
		t.Fatalf("OpenFileWithSchemaOptions failed: %v", err)
	}
	defer r.Close()

	// file1.txt - STANDARD_IA
	rec, err := r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.TierID != 1 { // StandardIA = 1
		t.Errorf("file1: got TierID=%d, want 1 (StandardIA)", rec.TierID)
	}

	// file2.txt - IT INFREQUENT
	rec, err = r.Read()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if rec.TierID != 8 { // ITInfrequent = 8
		t.Errorf("file2: got TierID=%d, want 8 (ITInfrequent)", rec.TierID)
	}
}
