package format

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ManifestVersion is the current manifest format version. Version 2
// checksums a file by its pieces (see checksumFiles).
const ManifestVersion = 2

// Manifest describes the contents of an index directory.
type Manifest struct {
	CreatedAt time.Time           `json:"created_at"`
	Files     map[string]FileInfo `json:"files"`
	Version   int                 `json:"version"`
	NodeCount uint64              `json:"node_count"`
	MaxDepth  uint32              `json:"max_depth"`
}

// FileInfo describes a single file in the index.
type FileInfo struct {
	Checksum string `json:"checksum"`
	Size     int64  `json:"size"`
}

// WriteManifest creates a manifest for all files in the index directory.
// The manifest enumerates every file (top-level index files plus the
// tier_stats subdirectory) with size + checksum so consumers can verify
// integrity and so callers can compute on-disk size by summing
// manifest.Files[*].Size — no directory walk needed.
//
// Progress, when non-nil, receives the StepChecksum step.
func WriteManifest(dir string, nodeCount uint64, maxDepth uint32, progress StepProgress) error {
	manifest := Manifest{
		Version:   ManifestVersion,
		CreatedAt: time.Now().UTC(),
		NodeCount: nodeCount,
		MaxDepth:  maxDepth,
		Files:     make(map[string]FileInfo),
	}
	names, err := manifestFileNames(dir)
	if err != nil {
		return err
	}
	var total int64
	for _, name := range names {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}

			return fmt.Errorf("stat %s: %w", name, err)
		}
		manifest.Files[name] = FileInfo{Size: info.Size()}
		total += info.Size()
	}
	sizes := make(map[string]int64, len(manifest.Files))
	for name, fi := range manifest.Files {
		sizes[name] = fi.Size
	}
	hashed := newStepCounter(progress, StepChecksum, total)
	sums, err := checksumFiles(dir, sizes, checksumPiece, hashed)
	if err != nil {
		return err
	}
	hashed.finish()
	for name, sum := range sums {
		fi := manifest.Files[name]
		fi.Checksum = sum
		manifest.Files[name] = fi
	}

	manifestPath := filepath.Join(dir, ManifestFile)
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	if err := WriteFileSync(manifestPath, data); err != nil {
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

// StepChecksum is WriteManifest's step: it counts the bytes hashed.
const StepChecksum = "Checksumming files"

// manifestFileNames lists the files a manifest covers. The top-level
// files are hard-coded, so the manifest can be verified against an
// expected set rather than whatever happened to land on disk; a file
// the build did not write is skipped when stat finds it missing.
// Tier-stats files live in a subdirectory and are named by tier
// FilePrefix, so the directory is listed: a new tier is captured
// automatically.
func manifestFileNames(dir string) ([]string, error) {
	names := []string{
		DepthOffsetsFile,
		DepthBlocksFile,
		MPHFile,
		MPHFSlotsFile,
		"tiers.json",
		CoreStatsFile,
		PrefixDictBlobFile,
		PrefixDictOffsetsFile,
		PrefixTreeFile,
		// Written only when the index needs them.
		CorePatchFile,
		CorePatchRowsFile,
	}
	entries, err := os.ReadDir(filepath.Join(dir, TierStatsDir))
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read tier_stats dir: %w", err)
	}
	for _, e := range entries {
		if e.Type().IsRegular() {
			names = append(names, filepath.Join(TierStatsDir, e.Name()))
		}
	}

	return names, nil
}

// ReadManifest reads the manifest from the index directory.
func ReadManifest(dir string) (*Manifest, error) {
	path := filepath.Join(dir, ManifestFile)
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

// ErrManifestVersion is returned for a manifest of another version,
// whose checksums this build computes differently.
var ErrManifestVersion = errors.New("unsupported manifest version")

// VerifyManifest checks that all files match their sizes and checksums.
func VerifyManifest(dir string, manifest *Manifest) error {
	if manifest.Version != ManifestVersion {
		return fmt.Errorf("%w: %d, want %d", ErrManifestVersion, manifest.Version, ManifestVersion)
	}
	sizes := make(map[string]int64, len(manifest.Files))
	for name, info := range manifest.Files {
		stat, err := os.Stat(filepath.Join(dir, name))
		if err != nil {
			return fmt.Errorf("file %s: %w", name, err)
		}
		if stat.Size() != info.Size {
			return fmt.Errorf("file %s: %w (got %d, want %d)",
				name, ErrSizeMismatch, stat.Size(), info.Size)
		}
		sizes[name] = info.Size
	}
	sums, err := checksumFiles(dir, sizes, checksumPiece, nil)
	if err != nil {
		return err
	}
	for name, info := range manifest.Files {
		if sums[name] != info.Checksum {
			return fmt.Errorf("file %s: %w", name, ErrChecksumMismatch)
		}
	}

	return nil
}

// WriteFileSync writes data to a file and fsyncs it.
func WriteFileSync(path string, data []byte) error {
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
