// Package s3fetch provides functionality to fetch AWS S3 Inventory files.
package s3fetch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// Sentinel errors surfaced by manifest parsing and S3 URI/ARN
// validation. Wrapped with %w so callers can errors.Is them out of a
// fmt.Errorf chain.
var (
	// ErrManifestMissingBucket is returned when a parsed manifest has no
	// destinationBucket field.
	ErrManifestMissingBucket = errors.New("manifest missing destinationBucket")

	// ErrManifestNoFiles is returned when a parsed manifest's files
	// array is empty.
	ErrManifestNoFiles = errors.New("manifest has no files")

	// ErrUnsupportedFileFormat is returned when the manifest declares a
	// fileFormat that isn't CSV or Parquet.
	ErrUnsupportedFileFormat = errors.New("unsupported file format")

	// ErrColumnNotFound is returned when a requested column name is
	// absent from the manifest's fileSchema.
	ErrColumnNotFound = errors.New("column not found in schema")

	// ErrEmptyBucketIdent is returned when ParseBucketIdentifier is
	// called with an empty string.
	ErrEmptyBucketIdent = errors.New("empty bucket identifier")

	// ErrBucketIdentIsURI is returned when the input to
	// ParseBucketIdentifier looks like a URI rather than a bare bucket
	// name or ARN.
	ErrBucketIdentIsURI = errors.New("bucket identifier looks like a URI, use ParseS3URI instead")

	// ErrInvalidARN is returned by parseBucketARN when the ARN's
	// structure is malformed (too few parts, missing prefix, wrong
	// service, etc.).
	ErrInvalidARN = errors.New("invalid ARN")

	// ErrInvalidS3URI is returned by ParseS3URI when the input is not
	// a well-formed s3:// URI.
	ErrInvalidS3URI = errors.New("invalid S3 URI")
)

// arnPartsRequired is the minimum number of colon-separated parts a
// valid S3 ARN carries: arn:partition:service:region:account:resource.
const arnPartsRequired = 6

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
		return ErrManifestMissingBucket
	}
	if len(m.Files) == 0 {
		return ErrManifestNoFiles
	}
	// Validate format - accept CSV, Parquet, or detect from file extensions.
	// If there's an explicit format declaration, it must be CSV or Parquet.
	if m.FileFormat != "" {
		upper := strings.ToUpper(m.FileFormat)
		if upper != "CSV" && upper != "PARQUET" {
			return fmt.Errorf("%w %q (supported: CSV, Parquet)", ErrUnsupportedFileFormat, m.FileFormat)
		}
	}

	return nil
}

// DetectFormat determines the inventory format based on the manifest's fileFormat
// field and file extensions. Priority:
//  1. Explicit fileFormat field ("CSV" or "Parquet")
//  2. File extension detection (.parquet, .csv, .csv.gz)
func (m *Manifest) DetectFormat() InventoryFormat {
	switch strings.ToUpper(m.FileFormat) {
	case "CSV":
		return InventoryFormatCSV
	case "PARQUET":
		return InventoryFormatParquet
	}

	if len(m.Files) > 0 {
		key := strings.ToLower(m.Files[0].Key)
		if strings.HasSuffix(key, ".parquet") {
			return InventoryFormatParquet
		}
		if strings.HasSuffix(key, ".csv") || strings.HasSuffix(key, ".csv.gz") {
			return InventoryFormatCSV
		}
	}

	// Default to CSV for backwards compatibility.
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

	return -1, fmt.Errorf("%w: column %q in schema %q", ErrColumnNotFound, name, m.FileSchema)
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
		return "", ErrEmptyBucketIdent
	}

	if strings.HasPrefix(bucketOrARN, "arn:") {
		return parseBucketARN(bucketOrARN)
	}

	// Plain bucket name - validate it doesn't contain obvious issues.
	if strings.Contains(bucketOrARN, "://") {
		return "", fmt.Errorf("%w: %q", ErrBucketIdentIsURI, bucketOrARN)
	}

	return bucketOrARN, nil
}

// parseBucketARN extracts the bucket name from an S3 bucket ARN.
// Valid S3 bucket ARN format: arn:aws:s3:::bucket-name
// The ARN has 6 colon-separated parts: arn:partition:service:region:account:resource
// For S3 bucket ARNs, region and account are empty, and resource is the bucket name.
func parseBucketARN(arn string) (string, error) {
	parts := strings.Split(arn, ":")
	if len(parts) < arnPartsRequired {
		return "", fmt.Errorf("%w %q: expected at least %d colon-separated parts", ErrInvalidARN, arn, arnPartsRequired)
	}

	if parts[0] != "arn" {
		return "", fmt.Errorf("%w %q: must start with 'arn:'", ErrInvalidARN, arn)
	}

	// parts[1] = partition (aws, aws-cn, aws-us-gov)
	// parts[2] = service (should be s3)
	if parts[2] != "s3" {
		return "", fmt.Errorf("%w %q: service must be 's3', got %q", ErrInvalidARN, arn, parts[2])
	}

	// parts[3] = region (empty for bucket ARNs)
	// parts[4] = account (empty for bucket ARNs)
	// parts[5] = resource (bucket name, possibly with more parts for access points)

	// For simple bucket ARNs like arn:aws:s3:::bucket-name, the resource is just the bucket name.
	// Join remaining parts in case there's additional path info.
	resource := strings.Join(parts[arnPartsRequired-1:], ":")
	if resource == "" {
		return "", fmt.Errorf("%w %q: missing bucket name", ErrInvalidARN, arn)
	}

	// The resource might contain a path for S3 access points, but for bucket ARNs it's just the bucket name.
	// Extract just the bucket name (first path component if there's a /).
	if idx := strings.Index(resource, "/"); idx >= 0 {
		resource = resource[:idx]
	}

	if resource == "" {
		return "", fmt.Errorf("%w %q: empty bucket name", ErrInvalidARN, arn)
	}

	return resource, nil
}

// S3URI is the bucket/key pair extracted from an s3:// URI. Returned
// as a struct so ParseS3URI has a single typed result instead of two
// stringly-typed positional ones (which gocritic flags as unnamed).
type S3URI struct {
	Bucket string
	Key    string
}

// ParseS3URI parses an S3 URI (s3://bucket/key).
func ParseS3URI(uri string) (S3URI, error) {
	if !strings.HasPrefix(uri, "s3://") {
		return S3URI{}, fmt.Errorf("%w: must start with s3://", ErrInvalidS3URI)
	}

	path := strings.TrimPrefix(uri, "s3://")
	const bucketKeyParts = 2
	parts := strings.SplitN(path, "/", bucketKeyParts)
	if len(parts) < 1 || parts[0] == "" {
		return S3URI{}, fmt.Errorf("%w: missing bucket name", ErrInvalidS3URI)
	}

	out := S3URI{Bucket: parts[0]}
	if len(parts) == bucketKeyParts {
		out.Key = parts[1]
	}

	return out, nil
}
