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

// Stage callback values come from the loader ("preparing", "done") and
// from the underlying pipeline ("init", "ingest", "merge"). The type
// stays unnamed (plain func(string)) so loader.Loader directly
// satisfies the inventory.IndexBuilder interface.

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

// CacheDirFor returns the on-disk path where an inventory's built index
// lives. Callers can also Stat it to check whether a build already exists.
func (l *Loader) CacheDirFor(srcBucket, invID string) string {
	return filepath.Join(l.cacheRoot, srcBucket, invID)
}

// Build is BuildWith with no stage callback.
func (l *Loader) Build(ctx context.Context, srcBucket, invID, manifestURI string) (string, error) {
	return l.BuildWith(ctx, srcBucket, invID, manifestURI, nil)
}

// BuildWith downloads the inventory referenced by manifestURI and
// produces a built index under CacheDirFor(srcBucket, invID). The
// onStage callback, if non-nil, receives stage transitions from the
// loader and the underlying pipeline. Partial builds are not safe to
// resume — the cache dir is cleared first.
func (l *Loader) BuildWith(ctx context.Context, srcBucket, invID, manifestURI string, onStage func(string)) (string, error) {
	if srcBucket == "" || invID == "" {
		return "", errEmptyID
	}
	if manifestURI == "" {
		return "", errEmptyManifest
	}
	if onStage == nil {
		onStage = func(string) {}
	}

	onStage("preparing")
	outDir := l.CacheDirFor(srcBucket, invID)
	if err := os.RemoveAll(outDir); err != nil {
		return "", fmt.Errorf("clear cache dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outDir), 0o755); err != nil {
		return "", fmt.Errorf("ensure cache parent: %w", err)
	}

	cfg := extsort.DefaultConfig()
	cfg.MemoryBudget = membudget.NewFromSystemRAM()
	cfg.OnPhase = onStage

	pipeline := extsort.NewPipeline(cfg, l.s3Client)
	if _, err := pipeline.Run(ctx, manifestURI, outDir); err != nil {
		// Leave the partial cache dir in place for diagnostics; the next
		// Build call will RemoveAll it.
		return "", fmt.Errorf("run pipeline: %w", err)
	}
	onStage("done")
	return outDir, nil
}

var (
	errEmptyID       = errors.New("source bucket and inventory id are required")
	errEmptyManifest = errors.New("manifest URI is required")
)
