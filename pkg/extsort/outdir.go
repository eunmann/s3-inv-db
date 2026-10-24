package extsort

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/eunmann/s3-inv-db/pkg/format"
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

// newStagingDir creates the hidden sibling directory the index is built
// in. Being a sibling keeps the final rename on one filesystem.
func newStagingDir(outDir string) (string, error) {
	parent := filepath.Dir(outDir)
	if err := os.MkdirAll(parent, format.DirPerm); err != nil {
		return "", fmt.Errorf("create output parent: %w", err)
	}
	dir, err := os.MkdirTemp(parent, "."+filepath.Base(outDir)+".building-")
	if err != nil {
		return "", fmt.Errorf("create staging dir: %w", err)
	}

	return dir, nil
}

// publishIndex moves a finished index from stage to outDir. A previous
// index at outDir is replaced (moved aside, then removed once the new
// one is in place); anything else at outDir is left alone and reported.
func publishIndex(stage, outDir string) error {
	if err := checkOutDir(outDir); err != nil {
		return err
	}
	parent := filepath.Dir(outDir)
	var old string
	if _, err := os.Stat(outDir); err == nil {
		// MkdirTemp picks a unique hidden name; os.Rename refuses to
		// replace a directory, so the placeholder is removed first.
		aside, err := os.MkdirTemp(parent, "."+filepath.Base(outDir)+".replaced-")
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
	if err := format.SyncDir(parent); err != nil {
		return fmt.Errorf("sync output parent: %w", err)
	}
	if old != "" {
		if err := os.RemoveAll(old); err != nil {
			return fmt.Errorf("remove previous index %s: %w", old, err)
		}
	}

	return nil
}
