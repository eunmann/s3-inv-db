// Package s3fetch provides functionality to fetch AWS S3 Inventory files.
package s3fetch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// InventoryFormat represents the format of S3 inventory files.
type InventoryFormat int

const (
	// InventoryFormatCSV indicates CSV-formatted inventory files.
	InventoryFormatCSV InventoryFormat = iota
	// InventoryFormatParquet indicates Parquet-formatted inventory files.
	InventoryFormatParquet
)

// Manifest represents an AWS S3 Inventory manifest.json file.
type Manifest struct {
	SourceBucket      string         `json:"sourceBucket"`
	DestinationBucket string         `json:"destinationBucket"`
	Version           string         `json:"version"`
	CreationTimestamp string         `json:"creationTimestamp"`
	FileFormat        string         `json:"fileFormat"`
	FileSchema        string         `json:"fileSchema"`
	Files             []ManifestFile `json:"files"`
}

// ManifestFile represents a single inventory file in the manifest.
type ManifestFile struct {
	Key         string `json:"key"`
	Size        int64  `json:"size"`
	MD5Checksum string `json:"MD5checksum"`
}

// ParseManifest parses an AWS S3 Inventory manifest.json.
func ParseManifest(r io.Reader) (*Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, fmt.Errorf("decode manifest: %w", err)
	}

	if err := m.validate(); err != nil {
		return nil, fmt.Errorf("validate manifest: %w", err)
	}

	return &m, nil
}

func (m *Manifest) validate() error {
	if m.DestinationBucket == "" {
		return errors.New("manifest missing destinationBucket")
	}
	if len(m.Files) == 0 {
		return errors.New("manifest has no files")
	}
	// Validate format - accept CSV, Parquet, or detect from file extensions
	// If there's an explicit format declaration, it must be CSV or Parquet
	if m.FileFormat != "" {
		upper := strings.ToUpper(m.FileFormat)
		if upper != "CSV" && upper != "PARQUET" {
			return fmt.Errorf("unsupported file format: %s (supported: CSV, Parquet)", m.FileFormat)
		}
	}
	return nil
}

// DetectFormat determines the inventory format based on the manifest's fileFormat
// field and file extensions. Priority:
//  1. Explicit fileFormat field ("CSV" or "Parquet")
//  2. File extension detection (.parquet, .csv, .csv.gz)
func (m *Manifest) DetectFormat() InventoryFormat {
	// Check explicit format declaration
	switch strings.ToUpper(m.FileFormat) {
	case "CSV":
		return InventoryFormatCSV
	case "PARQUET":
		return InventoryFormatParquet
	}

	// Fall back to file extension detection
	if len(m.Files) > 0 {
		key := strings.ToLower(m.Files[0].Key)
		if strings.HasSuffix(key, ".parquet") {
			return InventoryFormatParquet
		}
		if strings.HasSuffix(key, ".csv") || strings.HasSuffix(key, ".csv.gz") {
			return InventoryFormatCSV
		}
	}

	// Default to CSV for backwards compatibility
	return InventoryFormatCSV
}

// IsParquet returns true if the inventory format is Parquet.
func (m *Manifest) IsParquet() bool {
	return m.DetectFormat() == InventoryFormatParquet
}

// IsCSV returns true if the inventory format is CSV.
func (m *Manifest) IsCSV() bool {
	return m.DetectFormat() == InventoryFormatCSV
}

// GetDestinationBucketName returns the normalized bucket name from the DestinationBucket field.
// The DestinationBucket may be either a plain bucket name or an S3 ARN.
// This method extracts the bucket name suitable for S3 API calls.
func (m *Manifest) GetDestinationBucketName() (string, error) {
	return ParseBucketIdentifier(m.DestinationBucket)
}

// KeyColumnIndex returns the index of the Key column in the schema.
func (m *Manifest) KeyColumnIndex() (int, error) {
	return m.columnIndex("Key")
}

// SizeColumnIndex returns the index of the Size column in the schema.
func (m *Manifest) SizeColumnIndex() (int, error) {
	return m.columnIndex("Size")
}

// StorageClassColumnIndex returns the index of the StorageClass column in the schema.
// Returns -1 if the column is not present.
func (m *Manifest) StorageClassColumnIndex() int {
	idx, err := m.columnIndex("StorageClass")
	if err != nil {
		return -1
	}
	return idx
}

// AccessTierColumnIndex returns the index of the IntelligentTieringAccessTier column.
// Returns -1 if the column is not present.
func (m *Manifest) AccessTierColumnIndex() int {
	idx, err := m.columnIndex("IntelligentTieringAccessTier")
	if err != nil {
		return -1
	}
	return idx
}

func (m *Manifest) columnIndex(name string) (int, error) {
	cols := strings.Split(m.FileSchema, ",")
	for i, col := range cols {
		col = strings.TrimSpace(col)
		if strings.EqualFold(col, name) {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %q not found in schema: %s", name, m.FileSchema)
}

// ParseBucketIdentifier extracts the bucket name from either a plain bucket name
// or an S3 bucket ARN. AWS S3 Inventory manifests may contain the destination
// bucket as either format:
//   - Plain bucket name: "my-bucket"
//   - S3 bucket ARN: "arn:aws:s3:::my-bucket"
//
// Returns the bucket name suitable for use with S3 API calls.
func ParseBucketIdentifier(bucketOrARN string) (string, error) {
	if bucketOrARN == "" {
		return "", errors.New("empty bucket identifier")
	}

	// Check if it's an ARN
	if strings.HasPrefix(bucketOrARN, "arn:") {
		return parseBucketARN(bucketOrARN)
	}

	// Plain bucket name - validate it doesn't contain obvious issues
	if strings.Contains(bucketOrARN, "://") {
		return "", fmt.Errorf("invalid bucket identifier %q: looks like a URI, use ParseS3URI instead", bucketOrARN)
	}

	return bucketOrARN, nil
}

// parseBucketARN extracts the bucket name from an S3 bucket ARN.
// Valid S3 bucket ARN format: arn:aws:s3:::bucket-name
// The ARN has 6 colon-separated parts: arn:partition:service:region:account:resource
// For S3 bucket ARNs, region and account are empty, and resource is the bucket name.
func parseBucketARN(arn string) (string, error) {
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return "", fmt.Errorf("invalid ARN %q: expected at least 6 colon-separated parts", arn)
	}

	// Validate ARN structure
	if parts[0] != "arn" {
		return "", fmt.Errorf("invalid ARN %q: must start with 'arn:'", arn)
	}

	// parts[1] = partition (aws, aws-cn, aws-us-gov)
	// parts[2] = service (should be s3)
	if parts[2] != "s3" {
		return "", fmt.Errorf("invalid S3 ARN %q: service must be 's3', got %q", arn, parts[2])
	}

	// parts[3] = region (empty for bucket ARNs)
	// parts[4] = account (empty for bucket ARNs)
	// parts[5] = resource (bucket name, possibly with more parts for access points)

	// For simple bucket ARNs like arn:aws:s3:::bucket-name, the resource is just the bucket name
	// Join remaining parts in case there's additional path info
	resource := strings.Join(parts[5:], ":")
	if resource == "" {
		return "", fmt.Errorf("invalid S3 ARN %q: missing bucket name", arn)
	}

	// The resource might contain a path for S3 access points, but for bucket ARNs it's just the bucket name
	// Extract just the bucket name (first path component if there's a /)
	if idx := strings.Index(resource, "/"); idx >= 0 {
		resource = resource[:idx]
	}

	if resource == "" {
		return "", fmt.Errorf("invalid S3 ARN %q: empty bucket name", arn)
	}

	return resource, nil
}

// ParseS3URI parses an S3 URI (s3://bucket/key) into bucket and key components.
func ParseS3URI(uri string) (bucket, key string, err error) {
	if !strings.HasPrefix(uri, "s3://") {
		return "", "", errors.New("invalid S3 URI: must start with s3://")
	}

	path := strings.TrimPrefix(uri, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 1 || parts[0] == "" {
		return "", "", errors.New("invalid S3 URI: missing bucket name")
	}

	bucket = parts[0]
	if len(parts) == 2 {
		key = parts[1]
	}

	return bucket, key, nil
}
