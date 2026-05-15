// Package loader builds an indexread-compatible index from a remote S3
// Inventory manifest into a local cache directory.
package loader

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

// The progress callback receives stage transitions and per-chunk
// quantitative progress. Stage values: "preparing", "initializing",
// "downloading", "building", "done". done/total are 0 on plain stage
// transitions; non-zero while ingesting chunks (where total = total
// chunks, done = chunks processed). Unnamed func type keeps
// inventory.IndexBuilder satisfaction structural.

// cacheDirMode is the permission bits used for cache directories the
// loader creates. Owner-only access is the right default for a
// process-local cache.
const cacheDirMode = 0o750

// Loader runs the S3-inventory → on-disk-index build pipeline into a
// per-inventory subdirectory of the configured cache root.
type Loader struct {
	cacheRoot string
	s3Client  *s3fetch.Client
}

// New constructs a Loader. CacheRoot must already exist and be writable.
func New(cacheRoot string, s3Client *s3fetch.Client) *Loader {
	return &Loader{cacheRoot: cacheRoot, s3Client: s3Client}
}

// CacheDirFor returns the on-disk path where an inventory run's built
// index lives. The run timestamp keeps cache directories per-run, so
// multiple runs of the same inventory configuration can be loaded
// independently without clobbering each other.
func (l *Loader) CacheDirFor(srcBucket, invID, run string) string {
	return filepath.Join(l.cacheRoot, srcBucket, invID, run)
}

// Build is BuildWith with no stage callback.
func (l *Loader) Build(ctx context.Context, srcBucket, invID, run, manifestURI string) (string, error) {
	return l.BuildWith(ctx, srcBucket, invID, run, manifestURI, nil)
}

// BuildWith downloads the inventory referenced by manifestURI and
// produces a built index under CacheDirFor(srcBucket, invID, run). The
// onProgress callback, if non-nil, receives stage transitions and
// per-chunk quantitative progress for UI ETA. Partial builds are not
// safe to resume — the cache dir is cleared first.
func (l *Loader) BuildWith(ctx context.Context, srcBucket, invID, run, manifestURI string, onProgress func(stage string, done, total int64)) (string, error) {
	if srcBucket == "" || invID == "" || run == "" {
		return "", errEmptyID
	}
	if manifestURI == "" {
		return "", errEmptyManifest
	}
	if onProgress == nil {
		onProgress = func(string, int64, int64) {}
	}

	onProgress("preparing", 0, 0)
	outDir := l.CacheDirFor(srcBucket, invID, run)
	if err := os.RemoveAll(outDir); err != nil {
		return "", fmt.Errorf("clear cache dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outDir), cacheDirMode); err != nil {
		return "", fmt.Errorf("ensure cache parent: %w", err)
	}

	cfg := extsort.DefaultConfig()
	cfg.OnProgress = onProgress

	pipeline := extsort.NewPipeline(cfg, l.s3Client)
	if _, err := pipeline.Run(ctx, manifestURI, outDir); err != nil {
		// Leave the partial cache dir in place for diagnostics; the next
		// Build call will RemoveAll it.
		return "", fmt.Errorf("run pipeline: %w", err)
	}
	onProgress("done", 0, 0)

	return outDir, nil
}

// RemoveCache deletes a run's on-disk cache. Used by Unload to free
// disk after the in-memory index is released. Missing dirs are a no-op
// — callers don't have to check existence.
func (l *Loader) RemoveCache(srcBucket, invID, run string) error {
	if srcBucket == "" || invID == "" || run == "" {
		return errEmptyID
	}
	dir := l.CacheDirFor(srcBucket, invID, run)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove cache dir %s: %w", dir, err)
	}

	return nil
}

// CacheSizeBytes returns the total on-disk size of a run's cache dir.
// Walks every file and sums sizes. Returns 0, nil when the dir doesn't
// exist (not an error — the inventory simply has no cached index).
func (l *Loader) CacheSizeBytes(srcBucket, invID, run string) (int64, error) {
	dir := l.CacheDirFor(srcBucket, invID, run)
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}

			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}

		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("walk cache dir %s: %w", dir, err)
	}

	return total, nil
}

var (
	errEmptyID       = errors.New("source bucket, inventory id, and run are required")
	errEmptyManifest = errors.New("manifest URI is required")
)
