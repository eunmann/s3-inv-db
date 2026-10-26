package extsort

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/rs/zerolog"
)

// ErrOutDirNotIndex is returned when the output directory already holds
// files that are not an index. The pipeline only ever replaces a
// previous index; it never writes into, or deletes, anything else.
var ErrOutDirNotIndex = errors.New("output directory exists, is not empty and does not hold an index")

// checkOutDir reports whether outDir can receive an index: it is
// missing, empty, or holds a previous index (manifest.json present).
func checkOutDir(outDir string) error {
	info, err := os.Stat(outDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat output dir: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: %s is a file", ErrOutDirNotIndex, outDir)
	}
	entries, err := os.ReadDir(outDir)
	if err != nil {
		return fmt.Errorf("read output dir: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}
	if _, err := os.Stat(filepath.Join(outDir, format.ManifestFile)); err != nil {
		return fmt.Errorf("%w: %s", ErrOutDirNotIndex, outDir)
	}

	return nil
}

// Staging, replaced-index and scratch directories are hidden siblings
// of outDir named ".<base><suffix><random>", so a build can find the
// ones an earlier, killed build left behind.
const (
	stagingSuffix  = ".building-"
	replacedSuffix = ".replaced-"
	scratchSuffix  = ".scratch-"
)

// LeftoverBase reports whether name is a staging, replaced-index or
// scratch directory this package creates beside an output directory,
// and the output directory's base name if so.
func LeftoverBase(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, ".")
	if !ok {
		return "", false
	}
	for _, suffix := range []string{stagingSuffix, replacedSuffix, scratchSuffix} {
		if i := strings.LastIndex(rest, suffix); i > 0 {
			return rest[:i], true
		}
	}

	return "", false
}

// ReclaimStaleDirs is reclaimStaleDirs for callers outside a build:
// it removes the leftovers of killed builds beside outDir, moving a
// previous index back when outDir is missing.
func ReclaimStaleDirs(log *zerolog.Logger, outDir string) error {
	return reclaimStaleDirs(log, outDir)
}

// NewStagingDir is newStagingDir for callers that fill an output
// directory without a build, such as fetching a published index: the
// staging directory is reclaimed like a killed build's.
func NewStagingDir(outDir string) (string, error) { return newStagingDir(outDir) }

// PublishIndex is publishIndex for those callers.
func PublishIndex(log *zerolog.Logger, stage, outDir string) error {
	return publishIndex(log, stage, outDir)
}

// newScratchDir creates the hidden sibling directory a build's
// downloads, run files and MPHF scratch live in. Beside the output it
// shares the filesystem the operator sized for indexes (the server's
// cache volume), rather than os.TempDir: often tmpfs, so RAM the Go
// memory limit can't see, and outside the server's disk budget.
func newScratchDir(outDir string) (string, error) {
	outDir = filepath.Clean(outDir)
	parent, base := filepath.Dir(outDir), filepath.Base(outDir)
	if err := os.MkdirAll(parent, format.DirPerm); err != nil {
		return "", fmt.Errorf("create output parent: %w", err)
	}
	dir, err := os.MkdirTemp(parent, "."+base+scratchSuffix)
	if err != nil {
		return "", fmt.Errorf("create scratch dir: %w", err)
	}

	return dir, nil
}

// newStagingDir creates the hidden sibling directory the index is built
// in. Being a sibling keeps the final rename on one filesystem.
func newStagingDir(outDir string) (string, error) {
	// Cleaning keeps a trailing slash from making outDir its own parent.
	outDir = filepath.Clean(outDir)
	parent, base := filepath.Dir(outDir), filepath.Base(outDir)
	if err := os.MkdirAll(parent, format.DirPerm); err != nil {
		return "", fmt.Errorf("create output parent: %w", err)
	}
	dir, err := os.MkdirTemp(parent, "."+base+stagingSuffix)
	if err != nil {
		return "", fmt.Errorf("create staging dir: %w", err)
	}

	return dir, nil
}

// reclaimStaleDirs removes the staging, replaced-index and scratch
// directories a killed build left beside outDir, since its own cleanup
// never ran. A build killed between moving the previous index aside and
// publishing the new one leaves outDir missing; that previous index is
// moved back instead of removed. Builds into the same outDir must not
// overlap.
func reclaimStaleDirs(log *zerolog.Logger, outDir string) error {
	outDir = filepath.Clean(outDir)
	parent, base := filepath.Dir(outDir), filepath.Base(outDir)
	entries, err := os.ReadDir(parent)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read output parent: %w", err)
	}
	_, statErr := os.Stat(outDir)
	outDirMissing := errors.Is(statErr, fs.ErrNotExist)
	for _, e := range entries {
		name := e.Name()
		path := filepath.Join(parent, name)
		switch {
		case strings.HasPrefix(name, "."+base+replacedSuffix) && outDirMissing:
			if err := os.Rename(path, outDir); err != nil {
				return fmt.Errorf("restore previous index from %s: %w", path, err)
			}
			outDirMissing = false
			log.Warn().Str("path", path).Msg("restored previous index left aside by an interrupted build")
		case strings.HasPrefix(name, "."+base+replacedSuffix),
			strings.HasPrefix(name, "."+base+stagingSuffix),
			strings.HasPrefix(name, "."+base+scratchSuffix):
			logCleanupErr(log, "remove stale build dir", path, os.RemoveAll(path))
		}
	}

	return nil
}

// publishIndex moves a finished index from stage to outDir. A previous
// index at outDir is replaced (moved aside, then removed once the new
// one is in place); anything else at outDir is left alone and reported.
// Once the new index is in place the build has succeeded, so a failure
// to sync or to remove the previous index is only logged.
func publishIndex(log *zerolog.Logger, stage, outDir string) error {
	if err := checkOutDir(outDir); err != nil {
		return err
	}
	outDir = filepath.Clean(outDir)
	parent, base := filepath.Dir(outDir), filepath.Base(outDir)
	var old string
	if _, err := os.Stat(outDir); err == nil {
		// MkdirTemp picks a unique hidden name; os.Rename refuses to
		// replace a directory, so the placeholder is removed first.
		aside, err := os.MkdirTemp(parent, "."+base+replacedSuffix)
		if err != nil {
			return fmt.Errorf("reserve name for previous index: %w", err)
		}
		if err := os.Remove(aside); err != nil {
			return fmt.Errorf("reserve name for previous index: %w", err)
		}
		if err := os.Rename(outDir, aside); err != nil {
			return fmt.Errorf("move previous index aside: %w", err)
		}
		old = aside
	}
	if err := os.Rename(stage, outDir); err != nil {
		if old != "" {
			if restoreErr := os.Rename(old, outDir); restoreErr != nil {
				return errors.Join(fmt.Errorf("publish index: %w", err),
					fmt.Errorf("restore previous index from %s: %w", old, restoreErr))
			}
		}

		return fmt.Errorf("publish index: %w", err)
	}
	logCleanupErr(log, "sync output parent", parent, format.SyncDir(parent))
	if old != "" {
		logCleanupErr(log, "remove previous index", old, os.RemoveAll(old))
	}

	return nil
}
