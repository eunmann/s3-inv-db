// Package s3fetch provides functionality to fetch AWS S3 Inventory files.
package s3fetch

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
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
		return fmt.Errorf("manifest missing destinationBucket")
	}
	if len(m.Files) == 0 {
		return fmt.Errorf("manifest has no files")
	}
	if m.FileFormat != "CSV" {
		return fmt.Errorf("unsupported file format: %s (only CSV is currently supported)", m.FileFormat)
	}
	return nil
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

// ParseS3URI parses an S3 URI (s3://bucket/key) into bucket and key components.
func ParseS3URI(uri string) (bucket, key string, err error) {
	if !strings.HasPrefix(uri, "s3://") {
		return "", "", fmt.Errorf("invalid S3 URI: must start with s3://")
	}

	path := strings.TrimPrefix(uri, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 1 || parts[0] == "" {
		return "", "", fmt.Errorf("invalid S3 URI: missing bucket name")
	}

	bucket = parts[0]
	if len(parts) == 2 {
		key = parts[1]
	}

	return bucket, key, nil
}
