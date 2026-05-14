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
	"github.com/eunmann/s3-inv-db/pkg/membudget"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

// The progress callback receives stage transitions and per-chunk
// quantitative progress. Stage values: "preparing", "initializing",
// "downloading", "building", "done". done/total are 0 on plain stage
// transitions; non-zero while ingesting chunks (where total = total
// chunks, done = chunks processed). Unnamed func type keeps
// inventory.IndexBuilder satisfaction structural.

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
	if err := os.MkdirAll(filepath.Dir(outDir), 0o755); err != nil {
		return "", fmt.Errorf("ensure cache parent: %w", err)
	}

	cfg := extsort.DefaultConfig()
	cfg.MemoryBudget = membudget.NewFromSystemRAM()
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

var (
	errEmptyID       = errors.New("source bucket, inventory id, and run are required")
	errEmptyManifest = errors.New("manifest URI is required")
)
