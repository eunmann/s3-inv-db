package s3fetch

import (
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
			name: "valid parquet format",
			json: `{
				"destinationBucket": "inventory-bucket",
				"fileFormat": "Parquet",
				"fileSchema": "Key, Size",
				"files": [{"key": "file.parquet", "size": 100}]
			}`,
			wantErr:   false,
			wantFiles: 1,
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

func TestParseBucketIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantBucket string
		wantErr    bool
	}{
		{
			name:       "plain bucket name",
			input:      "my-bucket",
			wantBucket: "my-bucket",
		},
		{
			name:       "plain bucket name with dots",
			input:      "my.bucket.name",
			wantBucket: "my.bucket.name",
		},
		{
			name:       "plain bucket name with numbers",
			input:      "bucket-123",
			wantBucket: "bucket-123",
		},
		{
			name:       "s3 bucket ARN standard",
			input:      "arn:aws:s3:::my-bucket",
			wantBucket: "my-bucket",
		},
		{
			name:       "s3 bucket ARN with dashes",
			input:      "arn:aws:s3:::txg-s3-inventories",
			wantBucket: "txg-s3-inventories",
		},
		{
			name:       "s3 bucket ARN aws-cn partition",
			input:      "arn:aws-cn:s3:::my-china-bucket",
			wantBucket: "my-china-bucket",
		},
		{
			name:       "s3 bucket ARN aws-us-gov partition",
			input:      "arn:aws-us-gov:s3:::my-gov-bucket",
			wantBucket: "my-gov-bucket",
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "s3 URI instead of bucket name",
			input:   "s3://my-bucket/key",
			wantErr: true,
		},
		{
			name:    "https URI instead of bucket name",
			input:   "https://my-bucket.s3.amazonaws.com",
			wantErr: true,
		},
		{
			name:    "invalid ARN too few parts",
			input:   "arn:aws:s3",
			wantErr: true,
		},
		{
			name:    "non-s3 ARN",
			input:   "arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0",
			wantErr: true,
		},
		{
			name:    "ARN with empty bucket name",
			input:   "arn:aws:s3:::",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, err := ParseBucketIdentifier(tt.input)
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
		})
	}
}

func TestParseBucketIdentifier_RealWorldManifest(t *testing.T) {
	// Test case that matches the actual error from the user's manifest
	// The destinationBucket field in their manifest was "arn:aws:s3:::txg-s3-inventories"
	bucket, err := ParseBucketIdentifier("arn:aws:s3:::txg-s3-inventories")
	if err != nil {
		t.Fatalf("failed to parse real-world ARN: %v", err)
	}
	if bucket != "txg-s3-inventories" {
		t.Errorf("bucket = %q, want %q", bucket, "txg-s3-inventories")
	}
}

func TestManifest_GetDestinationBucketName(t *testing.T) {
	tests := []struct {
		name       string
		destBucket string
		wantBucket string
		wantErr    bool
	}{
		{
			name:       "plain bucket name",
			destBucket: "inventory-bucket",
			wantBucket: "inventory-bucket",
		},
		{
			name:       "ARN format",
			destBucket: "arn:aws:s3:::inventory-bucket",
			wantBucket: "inventory-bucket",
		},
		{
			name:       "real world ARN from error report",
			destBucket: "arn:aws:s3:::txg-s3-inventories",
			wantBucket: "txg-s3-inventories",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Manifest{DestinationBucket: tt.destBucket}
			bucket, err := m.GetDestinationBucketName()
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
		})
	}
}

func TestManifestDetectFormat(t *testing.T) {
	tests := []struct {
		name       string
		fileFormat string
		fileKey    string
		wantFormat InventoryFormat
		wantIsCSV  bool
	}{
		{
			name:       "explicit CSV format",
			fileFormat: "CSV",
			fileKey:    "file.csv.gz",
			wantFormat: InventoryFormatCSV,
			wantIsCSV:  true,
		},
		{
			name:       "explicit Parquet format",
			fileFormat: "Parquet",
			fileKey:    "file.parquet",
			wantFormat: InventoryFormatParquet,
			wantIsCSV:  false,
		},
		{
			name:       "explicit PARQUET format uppercase",
			fileFormat: "PARQUET",
			fileKey:    "file.parquet",
			wantFormat: InventoryFormatParquet,
			wantIsCSV:  false,
		},
		{
			name:       "detect from .parquet extension",
			fileFormat: "",
			fileKey:    "inventory/data/file.parquet",
			wantFormat: InventoryFormatParquet,
			wantIsCSV:  false,
		},
		{
			name:       "detect from .csv.gz extension",
			fileFormat: "",
			fileKey:    "inventory/data/file.csv.gz",
			wantFormat: InventoryFormatCSV,
			wantIsCSV:  true,
		},
		{
			name:       "detect from .csv extension",
			fileFormat: "",
			fileKey:    "file.csv",
			wantFormat: InventoryFormatCSV,
			wantIsCSV:  true,
		},
		{
			name:       "default to CSV when unknown",
			fileFormat: "",
			fileKey:    "file.unknown",
			wantFormat: InventoryFormatCSV,
			wantIsCSV:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Manifest{
				DestinationBucket: "bucket",
				FileFormat:        tt.fileFormat,
				FileSchema:        "Key, Size",
				Files:             []ManifestFile{{Key: tt.fileKey, Size: 100}},
			}

			gotFormat := m.DetectFormat()
			if gotFormat != tt.wantFormat {
				t.Errorf("DetectFormat() = %v, want %v", gotFormat, tt.wantFormat)
			}

			gotIsCSV := m.IsCSV()
			if gotIsCSV != tt.wantIsCSV {
				t.Errorf("IsCSV() = %v, want %v", gotIsCSV, tt.wantIsCSV)
			}

			gotIsParquet := m.IsParquet()
			if gotIsParquet != !tt.wantIsCSV {
				t.Errorf("IsParquet() = %v, want %v", gotIsParquet, !tt.wantIsCSV)
			}
		})
	}
}

// TestManifestIntegration_ARNvsBucketName tests the full flow of parsing a manifest
// with both ARN and plain bucket name formats.
func TestManifestIntegration_ARNvsBucketName(t *testing.T) {
	// This matches the real-world AWS S3 Inventory manifest structure
	// Case 1: destinationBucket is an ARN (the bug case)
	manifestWithARN := `{
		"sourceBucket": "10x-rapture-prod",
		"destinationBucket": "arn:aws:s3:::txg-s3-inventories",
		"version": "2016-11-30",
		"creationTimestamp": "1721876400000",
		"fileFormat": "CSV",
		"fileSchema": "Bucket, Key, Size, LastModifiedDate, ETag, StorageClass",
		"files": [
			{"key": "10x-rapture-prod/rapture-prod-weekly-inventory/data/2d9ba0b6-fb57-4e28-9315-70e4317a12b2.csv.gz", "size": 12345678, "MD5checksum": "abc123def456"}
		]
	}`

	// Case 2: destinationBucket is a plain bucket name (no bug)
	manifestWithPlainName := `{
		"sourceBucket": "10x-rapture-prod",
		"destinationBucket": "txg-s3-inventories",
		"version": "2016-11-30",
		"creationTimestamp": "1721876400000",
		"fileFormat": "CSV",
		"fileSchema": "Bucket, Key, Size, LastModifiedDate, ETag, StorageClass",
		"files": [
			{"key": "10x-rapture-prod/rapture-prod-weekly-inventory/data/2d9ba0b6-fb57-4e28-9315-70e4317a12b2.csv.gz", "size": 12345678, "MD5checksum": "abc123def456"}
		]
	}`

	tests := []struct {
		name         string
		manifestJSON string
		wantBucket   string
		wantKey      string
	}{
		{
			name:         "ARN destination bucket",
			manifestJSON: manifestWithARN,
			wantBucket:   "txg-s3-inventories",
			wantKey:      "10x-rapture-prod/rapture-prod-weekly-inventory/data/2d9ba0b6-fb57-4e28-9315-70e4317a12b2.csv.gz",
		},
		{
			name:         "plain bucket name destination",
			manifestJSON: manifestWithPlainName,
			wantBucket:   "txg-s3-inventories",
			wantKey:      "10x-rapture-prod/rapture-prod-weekly-inventory/data/2d9ba0b6-fb57-4e28-9315-70e4317a12b2.csv.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifest, err := ParseManifest(strings.NewReader(tt.manifestJSON))
			if err != nil {
				t.Fatalf("failed to parse manifest: %v", err)
			}

			// Verify we can get the normalized bucket name
			bucketName, err := manifest.GetDestinationBucketName()
			if err != nil {
				t.Fatalf("failed to get destination bucket name: %v", err)
			}

			if bucketName != tt.wantBucket {
				t.Errorf("bucket = %q, want %q", bucketName, tt.wantBucket)
			}

			// Verify we have the correct file key
			if len(manifest.Files) != 1 {
				t.Fatalf("expected 1 file, got %d", len(manifest.Files))
			}
			if manifest.Files[0].Key != tt.wantKey {
				t.Errorf("file key = %q, want %q", manifest.Files[0].Key, tt.wantKey)
			}

			// This is the critical assertion: the bucket name we would use for S3 API calls
			// should NEVER be an ARN or look like "arn:aws:s3:::bucket"
			if strings.HasPrefix(bucketName, "arn:") {
				t.Errorf("bucket name should be normalized, but got ARN: %q", bucketName)
			}

			// And we should never construct URLs like "s3://arn:aws:s3:::bucket/key"
			constructedURL := "s3://" + bucketName + "/" + manifest.Files[0].Key
			if strings.Contains(constructedURL, "arn:aws:s3") {
				t.Errorf("constructed URL should not contain ARN, but got: %q", constructedURL)
			}

			// The correct URL format
			expectedURL := "s3://txg-s3-inventories/10x-rapture-prod/rapture-prod-weekly-inventory/data/2d9ba0b6-fb57-4e28-9315-70e4317a12b2.csv.gz"
			if constructedURL != expectedURL {
				t.Errorf("URL = %q, want %q", constructedURL, expectedURL)
			}
		})
	}
}
