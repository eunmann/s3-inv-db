package s3fetch

import (
	"os"
	"strings"
	"testing"
)

func TestParseManifest(t *testing.T) {
	tests := []struct {
		name      string
		json      string
		wantErr   bool
		wantFiles int
	}{
		{
			name: "valid manifest",
			json: `{
				"sourceBucket": "my-bucket",
				"destinationBucket": "inventory-bucket",
				"version": "2016-11-30",
				"fileFormat": "CSV",
				"fileSchema": "Bucket, Key, Size, LastModifiedDate",
				"files": [
					{"key": "data/file1.csv.gz", "size": 1234, "MD5checksum": "abc123"},
					{"key": "data/file2.csv.gz", "size": 5678, "MD5checksum": "def456"}
				]
			}`,
			wantErr:   false,
			wantFiles: 2,
		},
		{
			name: "missing destination bucket",
			json: `{
				"sourceBucket": "my-bucket",
				"fileFormat": "CSV",
				"fileSchema": "Bucket, Key, Size",
				"files": [{"key": "file.csv", "size": 100}]
			}`,
			wantErr: true,
		},
		{
			name: "no files",
			json: `{
				"destinationBucket": "inventory-bucket",
				"fileFormat": "CSV",
				"fileSchema": "Key, Size",
				"files": []
			}`,
			wantErr: true,
		},
		{
			name: "unsupported format ORC",
			json: `{
				"destinationBucket": "inventory-bucket",
				"fileFormat": "ORC",
				"fileSchema": "Key, Size",
				"files": [{"key": "file.orc", "size": 100}]
			}`,
			wantErr: true,
		},
		{
			name: "unsupported format Parquet",
			json: `{
				"destinationBucket": "inventory-bucket",
				"fileFormat": "Parquet",
				"fileSchema": "Key, Size",
				"files": [{"key": "file.parquet", "size": 100}]
			}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := ParseManifest(strings.NewReader(tt.json))
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(m.Files) != tt.wantFiles {
				t.Errorf("got %d files, want %d", len(m.Files), tt.wantFiles)
			}
		})
	}
}

func TestManifestColumnIndex(t *testing.T) {
	manifest := &Manifest{
		FileSchema: "Bucket, Key, Size, LastModifiedDate, ETag",
	}

	keyIdx, err := manifest.KeyColumnIndex()
	if err != nil {
		t.Fatalf("KeyColumnIndex failed: %v", err)
	}
	if keyIdx != 1 {
		t.Errorf("KeyColumnIndex = %d, want 1", keyIdx)
	}

	sizeIdx, err := manifest.SizeColumnIndex()
	if err != nil {
		t.Fatalf("SizeColumnIndex failed: %v", err)
	}
	if sizeIdx != 2 {
		t.Errorf("SizeColumnIndex = %d, want 2", sizeIdx)
	}
}

func TestManifestColumnIndex_CaseInsensitive(t *testing.T) {
	manifest := &Manifest{
		FileSchema: "bucket, KEY, SIZE, lastmodifieddate",
	}

	keyIdx, err := manifest.KeyColumnIndex()
	if err != nil {
		t.Fatalf("KeyColumnIndex failed: %v", err)
	}
	if keyIdx != 1 {
		t.Errorf("KeyColumnIndex = %d, want 1", keyIdx)
	}
}

func TestManifestColumnIndex_NotFound(t *testing.T) {
	manifest := &Manifest{
		FileSchema: "Bucket, ObjectKey, ObjectSize",
	}

	_, err := manifest.KeyColumnIndex()
	if err == nil {
		t.Error("expected error for missing Key column")
	}

	_, err = manifest.SizeColumnIndex()
	if err == nil {
		t.Error("expected error for missing Size column")
	}
}

func TestParseS3URI(t *testing.T) {
	tests := []struct {
		uri        string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{
			uri:        "s3://my-bucket/path/to/manifest.json",
			wantBucket: "my-bucket",
			wantKey:    "path/to/manifest.json",
		},
		{
			uri:        "s3://bucket/key",
			wantBucket: "bucket",
			wantKey:    "key",
		},
		{
			uri:        "s3://bucket-only/",
			wantBucket: "bucket-only",
			wantKey:    "",
		},
		{
			uri:        "s3://bucket",
			wantBucket: "bucket",
			wantKey:    "",
		},
		{
			uri:     "https://bucket/key",
			wantErr: true,
		},
		{
			uri:     "/local/path",
			wantErr: true,
		},
		{
			uri:     "s3://",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.uri, func(t *testing.T) {
			bucket, key, err := ParseS3URI(tt.uri)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if bucket != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", bucket, tt.wantBucket)
			}
			if key != tt.wantKey {
				t.Errorf("key = %q, want %q", key, tt.wantKey)
			}
		})
	}
}

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple.csv", "simple.csv"},
		{"path/to/file.csv.gz", "file.csv.gz"},
		{"inventory/bucket/2024/01/file.csv", "file.csv"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeFilename(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeFilename(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestManifestStorageClassColumnIndex(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   int
	}{
		{
			name:   "storage class present",
			schema: "Bucket, Key, Size, StorageClass",
			want:   3,
		},
		{
			name:   "storage class absent",
			schema: "Bucket, Key, Size",
			want:   -1,
		},
		{
			name:   "case insensitive",
			schema: "key, size, storageclass",
			want:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Manifest{FileSchema: tt.schema}
			got := m.StorageClassColumnIndex()
			if got != tt.want {
				t.Errorf("StorageClassColumnIndex() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestManifestAccessTierColumnIndex(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   int
	}{
		{
			name:   "access tier present",
			schema: "Key, Size, IntelligentTieringAccessTier",
			want:   2,
		},
		{
			name:   "access tier absent",
			schema: "Key, Size, StorageClass",
			want:   -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Manifest{FileSchema: tt.schema}
			got := m.AccessTierColumnIndex()
			if got != tt.want {
				t.Errorf("AccessTierColumnIndex() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestNewFetcherDefaults(t *testing.T) {
	// NewFetcher should set default concurrency when 0 or negative
	tests := []struct {
		name        string
		concurrency int
		want        int
	}{
		{"zero uses default", 0, 4},
		{"negative uses default", -1, 4},
		{"positive preserved", 8, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := FetchConfig{Concurrency: tt.concurrency}
			f := NewFetcher(nil, cfg)
			if f.cfg.Concurrency != tt.want {
				t.Errorf("Concurrency = %d, want %d", f.cfg.Concurrency, tt.want)
			}
		})
	}
}

func TestFetcherCleanupKeepFiles(t *testing.T) {
	tmpDir := t.TempDir()
	testDir := tmpDir + "/downloads"

	// Create the directory
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a test file
	if err := os.WriteFile(testDir+"/test.txt", []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	// Cleanup with KeepFiles=true should not remove files
	f := NewFetcher(nil, FetchConfig{
		DownloadDir: testDir,
		KeepFiles:   true,
	})

	if err := f.Cleanup(); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	// Directory should still exist
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Error("Cleanup removed directory despite KeepFiles=true")
	}
}

func TestFetcherCleanupRemovesFiles(t *testing.T) {
	tmpDir := t.TempDir()
	testDir := tmpDir + "/downloads"

	// Create the directory
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a test file
	if err := os.WriteFile(testDir+"/test.txt", []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	// Cleanup with KeepFiles=false should remove files
	f := NewFetcher(nil, FetchConfig{
		DownloadDir: testDir,
		KeepFiles:   false,
	})

	if err := f.Cleanup(); err != nil {
		t.Fatalf("Cleanup failed: %v", err)
	}

	// Directory should be removed
	if _, err := os.Stat(testDir); !os.IsNotExist(err) {
		t.Error("Cleanup did not remove directory")
	}
}
