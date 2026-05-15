package format

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// ManifestVersion is the current manifest format version.
const ManifestVersion = 1

// Manifest describes the contents of an index directory.
type Manifest struct {
	Version   int                 `json:"version"`
	CreatedAt time.Time           `json:"created_at"`
	NodeCount uint64              `json:"node_count"`
	MaxDepth  uint32              `json:"max_depth"`
	Files     map[string]FileInfo `json:"files"`
}

// FileInfo describes a single file in the index.
type FileInfo struct {
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"` // SHA-256 hex
}

// WriteManifest creates a manifest for all files in the index directory.
// The manifest enumerates every file (top-level index files plus the
// tier_stats subdirectory) with size + checksum so consumers can verify
// integrity and so callers can compute on-disk size by summing
// manifest.Files[*].Size — no directory walk needed.
func WriteManifest(dir string, nodeCount uint64, maxDepth uint32) error {
	manifest := Manifest{
		Version:   ManifestVersion,
		CreatedAt: time.Now().UTC(),
		NodeCount: nodeCount,
		MaxDepth:  maxDepth,
		Files:     make(map[string]FileInfo),
	}

	expectedTopLevel := []string{
		"subtree_end.u64",
		"depth.u32",
		"object_count.u64",
		"total_bytes.u64",
		"max_depth_in_subtree.u32",
		"depth_offsets.u64",
		"depth_positions.u64",
		"mph.bin",
		"mph_fp.u64",
		"mph_pos.u64",
		"prefix_blob.bin",
		"prefix_offsets.u64",
		SegmentsBlobFile,
		SegmentsOffsetsFile,
		PrefixSegIDsFile,
		PrefixSegOffsetsFile,
		"tiers.json",
	}

	for _, name := range expectedTopLevel {
		if err := addFile(dir, name, manifest.Files); err != nil {
			return err
		}
	}

	// Tier-stats files live in a subdirectory and are named by tier
	// FilePrefix; enumerate the directory rather than hard-coding the
	// list so a new tier added later is captured automatically.
	tierDir := filepath.Join(dir, "tier_stats")
	if entries, err := os.ReadDir(tierDir); err == nil {
		for _, e := range entries {
			if e.IsDir() || !e.Type().IsRegular() {
				continue
			}
			name := filepath.Join("tier_stats", e.Name())
			if err := addFile(dir, name, manifest.Files); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read tier_stats dir: %w", err)
	}

	manifestPath := filepath.Join(dir, "manifest.json")
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	if err := writeFileSync(manifestPath, data); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	return nil
}

// TotalBytes sums the size of every file the manifest enumerates. The
// budget tracker and on-disk-size readout use this to avoid walking
// the directory on every load.
func (m *Manifest) TotalBytes() uint64 {
	var total uint64
	for _, f := range m.Files {
		if f.Size > 0 {
			total += uint64(f.Size)
		}
	}
	return total
}

func addFile(root, name string, dst map[string]FileInfo) error {
	path := filepath.Join(root, name)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("stat %s: %w", name, err)
	}
	checksum, err := checksumFile(path)
	if err != nil {
		return fmt.Errorf("checksum %s: %w", name, err)
	}
	dst[name] = FileInfo{Size: info.Size(), Checksum: checksum}
	return nil
}

// ReadManifest reads the manifest from the index directory.
func ReadManifest(dir string) (*Manifest, error) {
	path := filepath.Join(dir, "manifest.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	return &manifest, nil
}

// VerifyManifest checks that all files match their checksums.
func VerifyManifest(dir string, manifest *Manifest) error {
	for name, info := range manifest.Files {
		path := filepath.Join(dir, name)

		// Check file exists
		stat, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("file %s: %w", name, err)
		}

		// Check size
		if stat.Size() != info.Size {
			return fmt.Errorf("file %s: size mismatch (got %d, want %d)",
				name, stat.Size(), info.Size)
		}

		// Check checksum
		checksum, err := checksumFile(path)
		if err != nil {
			return fmt.Errorf("checksum %s: %w", name, err)
		}

		if checksum != info.Checksum {
			return fmt.Errorf("file %s: checksum mismatch", name)
		}
	}

	return nil
}

// checksumFile computes the SHA-256 checksum of a file.
func checksumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("read file: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// writeFileSync writes data to a file and fsyncs it.
func writeFileSync(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write file: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}
	return nil
}

// SyncDir fsyncs a directory to ensure entries are persisted.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open directory: %w", err)
	}
	defer d.Close()

	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}
