package format

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// ManifestVersion is the current manifest format version. Version 3
// checksums a file by its pieces' CRC-32Cs (see checksumFiles);
// version 2's were SHA-256.
const ManifestVersion = 3

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
// A file's checksum is the one its writer recorded in sums while
// writing it; a file with none there, or changed since, is read and
// hashed. It returns the names of the files it read.
//
// Progress, when non-nil, receives the StepChecksum step.
func WriteManifest(dir string, nodeCount uint64, maxDepth uint32, sums *Checksums, progress StepProgress) ([]string, error) {
	manifest := Manifest{
		Version:   ManifestVersion,
		CreatedAt: time.Now().UTC(),
		NodeCount: nodeCount,
		MaxDepth:  maxDepth,
		Files:     make(map[string]FileInfo),
	}
	names, err := manifestFileNames(dir)
	if err != nil {
		return nil, err
	}
	reread := make(map[string]int64)
	var total, recorded int64
	for _, name := range names {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}

			return nil, fmt.Errorf("stat %s: %w", name, err)
		}
		total += info.Size()
		sum, ok := sums.lookup(path, info)
		manifest.Files[name] = FileInfo{Size: info.Size(), Checksum: sum}
		if ok {
			recorded += info.Size()
		} else {
			reread[name] = info.Size()
		}
	}
	hashed := newStepCounter(progress, StepChecksum, total)
	hashed.add(int(recorded))
	reads, err := checksumFiles(dir, reread, checksumPiece, hashed)
	if err != nil {
		return nil, err
	}
	hashed.finish()
	for name, sum := range reads {
		fi := manifest.Files[name]
		fi.Checksum = sum
		manifest.Files[name] = fi
	}

	manifestPath := filepath.Join(dir, ManifestFile)
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}

	if err := os.WriteFile(manifestPath, data, FilePerm); err != nil {
		return nil, fmt.Errorf("write manifest: %w", err)
	}

	return slices.Sorted(maps.Keys(reread)), nil
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

// StepChecksum is WriteManifest's step: it counts the bytes of the
// files checksummed, those their writers hashed all at once.
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
		tiers.ManifestFile,
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
