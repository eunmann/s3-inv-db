// Package loader builds an indexread-compatible index from a remote S3
// Inventory manifest into a local cache directory.
package loader

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexstore"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/rs/zerolog"
)

//nolint:gochecknoglobals // shared no-op closure so BuildWith doesn't allocate per call
var noopProgress = func(string, int64, int64) {}

// Loader materialises an inventory run's index in a per-run
// subdirectory of the configured cache root: by downloading the index
// a build published to the index source, or by running the
// S3-inventory → index build pipeline itself.
type Loader struct {
	s3Client  *s3fetch.Client
	published *indexstore.Store
	cacheRoot string
	pubBase   string
	noBuild   bool
}

// Option configures a Loader.
type Option func(*Loader)

// WithIndexSource makes the loader fetch a run's index from store under
// base when one is published there. Without build, a run that isn't
// published fails instead of being built in-process: the server has
// only an index source to read.
func WithIndexSource(store *indexstore.Store, base string, build bool) Option {
	return func(l *Loader) {
		l.published, l.pubBase, l.noBuild = store, base, !build
	}
}

// New constructs a Loader. CacheRoot must already exist and be writable.
func New(cacheRoot string, s3Client *s3fetch.Client, opts ...Option) *Loader {
	l := &Loader{cacheRoot: cacheRoot, s3Client: s3Client}
	for _, opt := range opts {
		opt(l)
	}

	return l
}

// CacheDirFor returns the on-disk path where an inventory run's built
// index lives. The run timestamp keeps cache directories per-run, so
// multiple runs of the same inventory configuration can be loaded
// independently without clobbering each other. Callers RemoveAll the
// result, so every key segment must be a single plain path element:
// anything that would resolve outside <root>/<src>/<id>/ is refused.
func (l *Loader) CacheDirFor(key inventory.CacheKey) (string, error) {
	if !key.Valid() {
		return "", errEmptyID
	}
	for _, seg := range []string{key.SourceBucket, key.InventoryID, key.Run} {
		if seg == "." || seg == ".." || strings.ContainsAny(seg, `/\`) {
			return "", fmt.Errorf("%w: %q", errUnsafeCacheKey, seg)
		}
	}

	return filepath.Join(l.cacheRoot, key.SourceBucket, key.InventoryID, key.Run), nil
}

// BuildWith produces the run's index under CacheDirFor(key): fetched
// from the index source when a build published it there, otherwise
// built from the inventory referenced by manifestURI. The onProgress
// callback, if non-nil, receives stage transitions and per-chunk
// quantitative progress for UI ETA. The eventBus, if non-nil, is
// plumbed to the pipeline so subscribers (e.g. jobs.Recorder) can
// build a fine-grained stage timeline. The run's cache dir is cleared
// first: only a run that isn't served is built (inventory.State.CanLoad)
// and a load always rebuilds, so a previous index there has no reader
// and would only hold disk the budget doesn't count, twice over during
// the rebuild. Keeping it would pay off only with reuse, which would
// need the index to record the code that built it.
func (l *Loader) BuildWith(ctx context.Context, key inventory.CacheKey, manifestURI string, onProgress func(stage string, done, total int64), eventBus *events.Bus) (string, error) {
	outDir, err := l.CacheDirFor(key)
	if err != nil {
		return "", err
	}
	if manifestURI == "" {
		return "", errEmptyManifest
	}
	if onProgress == nil {
		onProgress = noopProgress
	}

	onProgress("preparing", 0, 0)
	if err := os.RemoveAll(outDir); err != nil {
		return "", fmt.Errorf("clear cache dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outDir), format.DirPerm); err != nil {
		return "", fmt.Errorf("ensure cache parent: %w", err)
	}

	if l.published != nil {
		err := l.fetchPublished(ctx, key, outDir, onProgress)
		switch {
		case err == nil:
			return outDir, nil
		case !errors.Is(err, indexstore.ErrNotPublished) || l.noBuild:
			return "", err
		}
	}

	cfg := extsort.DefaultConfig()
	cfg.Observe.OnProgress = onProgress
	cfg.Observe.EventBus = eventBus

	pipeline := extsort.NewPipeline(cfg, l.s3Client)
	if _, err := pipeline.Run(ctx, manifestURI, outDir); err != nil {
		// The pipeline builds in a staging dir and removes it on
		// failure, so no partial index is left at outDir.
		return "", fmt.Errorf("run pipeline: %w", err)
	}
	// Completion is signalled by the scheduler's terminal state flip
	// (succeeded/failed/cancelled) and closeOpenStages; no "done"
	// sentinel — emitting one here created a transient race where
	// job.Stage = "done" while job.State was still Running.

	return outDir, nil
}

// fetchPublished downloads the run's published index into a staging
// directory beside outDir, verifies it against its manifest and moves
// it into place. It returns ErrNotPublished when the run has none.
func (l *Loader) fetchPublished(ctx context.Context, key inventory.CacheKey, outDir string, onProgress func(stage string, done, total int64)) error {
	prefix := indexstore.Run{Source: key.SourceBucket, Inventory: key.InventoryID, Run: key.Run}.Prefix(l.pubBase)
	stage, err := extsort.NewStagingDir(outDir)
	if err != nil {
		return fmt.Errorf("stage published index: %w", err)
	}
	onProgress("downloading", 0, 0)
	err = l.published.Fetch(ctx, prefix, stage, func(done, total uint64) {
		onProgress("downloading", int64(done), int64(total))
	})
	if err != nil {
		return errors.Join(err, os.RemoveAll(stage))
	}
	if err := extsort.PublishIndex(zerolog.Ctx(ctx), stage, outDir); err != nil {
		return errors.Join(fmt.Errorf("move fetched index into place: %w", err), os.RemoveAll(stage))
	}

	return nil
}

// RemoveCache deletes a run's on-disk cache. Used by Unload to free
// disk after the in-memory index is released. Missing dirs are a no-op
// — callers don't have to check existence.
func (l *Loader) RemoveCache(key inventory.CacheKey) error {
	dir, err := l.CacheDirFor(key)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove cache dir %s: %w", dir, err)
	}

	return nil
}

// CacheSizeBytes returns 0 (not an error) when the cache dir is absent.
func (l *Loader) CacheSizeBytes(key inventory.CacheKey) (int64, error) {
	dir, err := l.CacheDirFor(key)
	if err != nil {
		return 0, err
	}
	var total int64
	err = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}

			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}

			return fmt.Errorf("stat entry: %w", err)
		}
		total += info.Size()

		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		return 0, fmt.Errorf("walk cache dir %s: %w", dir, err)
	}

	return total, nil
}

var (
	errEmptyID        = errors.New("source bucket, inventory id, and run are required")
	errEmptyManifest  = errors.New("manifest URI is required")
	errUnsafeCacheKey = errors.New("cache key segment is not a plain path element")
)

// Reconcile cleans the cache root at startup, removing only what the
// server provably created: the leftovers of killed builds (handled as
// a build would, so a previous index moved aside is restored rather
// than lost), and run directories holding an index (their manifest
// parses) for runs keep reports false. Anything else under the root is
// left alone, so a cache root pointed at a directory with other content
// loses nothing. It returns the run directories it removed.
func (l *Loader) Reconcile(log *zerolog.Logger, keep func(inventory.CacheKey) bool) ([]string, error) {
	var removed []string
	sources, err := readDirs(l.cacheRoot)
	if err != nil {
		return nil, err
	}
	for _, src := range sources {
		invs, err := readDirs(filepath.Join(l.cacheRoot, src))
		if err != nil {
			return removed, err
		}
		for _, inv := range invs {
			invDir := filepath.Join(l.cacheRoot, src, inv)
			if err := reclaimLeftovers(log, invDir); err != nil {
				return removed, err
			}
			runs, err := readDirs(invDir)
			if err != nil {
				return removed, err
			}
			for _, run := range runs {
				dir := filepath.Join(invDir, run)
				if strings.HasPrefix(run, ".") || keep(inventory.CacheKey{SourceBucket: src, InventoryID: inv, Run: run}) {
					continue
				}
				if !isIndexDir(dir) {
					continue // not an index this server wrote
				}
				if err := os.RemoveAll(dir); err != nil {
					return removed, fmt.Errorf("remove %s: %w", dir, err)
				}
				removed = append(removed, dir)
			}
		}
	}

	return removed, nil
}

// isIndexDir reports whether dir holds an index this project wrote: a
// manifest that parses and lists the core stats file. Any format
// version qualifies; an unrelated manifest.json does not.
func isIndexDir(dir string) bool {
	m, err := format.ReadManifest(dir)
	if err != nil {
		return false
	}
	_, ok := m.Files[format.CoreStatsFile]

	return ok
}

// reclaimLeftovers runs the pipeline's own stale-directory reclaim for
// every run in invDir that has build leftovers beside it.
func reclaimLeftovers(log *zerolog.Logger, invDir string) error {
	names, err := readDirs(invDir)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, name := range names {
		base, ok := extsort.LeftoverBase(name)
		if !ok || seen[base] {
			continue
		}
		seen[base] = true
		if err := extsort.ReclaimStaleDirs(log, filepath.Join(invDir, base)); err != nil {
			return fmt.Errorf("reclaim build leftovers in %s: %w", invDir, err)
		}
	}

	return nil
}

// readDirs lists the subdirectory names of dir; a missing dir has none.
func readDirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}

	return names, nil
}
