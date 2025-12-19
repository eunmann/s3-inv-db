package s3fetch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
)

// FetchConfig configures the inventory fetch operation.
type FetchConfig struct {
	// ManifestURI is the S3 URI to the manifest.json file.
	ManifestURI string
	// DownloadDir is the local directory to download inventory files to.
	DownloadDir string
	// Concurrency is the number of parallel downloads (default: 4).
	Concurrency int
	// KeepFiles if true, don't delete downloaded files after processing.
	KeepFiles bool
}

// FetchResult contains the results of fetching inventory files.
type FetchResult struct {
	// Manifest is the parsed manifest.
	Manifest *Manifest
	// LocalFiles are the paths to downloaded inventory files.
	LocalFiles []string
	// KeyColumn is the index of the Key column.
	KeyColumn int
	// SizeColumn is the index of the Size column.
	SizeColumn int
}

// Fetcher downloads S3 inventory files.
type Fetcher struct {
	client *Client
	cfg    FetchConfig
}

// NewFetcher creates a new inventory fetcher.
func NewFetcher(client *Client, cfg FetchConfig) *Fetcher {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	return &Fetcher{
		client: client,
		cfg:    cfg,
	}
}

// Fetch downloads the manifest and all inventory files.
func (f *Fetcher) Fetch(ctx context.Context) (*FetchResult, error) {
	// Parse manifest URI
	bucket, key, err := ParseS3URI(f.cfg.ManifestURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest URI: %w", err)
	}

	// Fetch and parse manifest
	manifest, err := f.client.FetchManifest(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}

	// Get column indices
	keyCol, err := manifest.KeyColumnIndex()
	if err != nil {
		return nil, err
	}
	sizeCol, err := manifest.SizeColumnIndex()
	if err != nil {
		return nil, err
	}

	// Create download directory
	if err := os.MkdirAll(f.cfg.DownloadDir, 0755); err != nil {
		return nil, fmt.Errorf("create download dir: %w", err)
	}

	// Download all inventory files concurrently
	localFiles, err := f.downloadFiles(ctx, manifest)
	if err != nil {
		return nil, err
	}

	return &FetchResult{
		Manifest:   manifest,
		LocalFiles: localFiles,
		KeyColumn:  keyCol,
		SizeColumn: sizeCol,
	}, nil
}

func (f *Fetcher) downloadFiles(ctx context.Context, manifest *Manifest) ([]string, error) {
	localFiles := make([]string, len(manifest.Files))
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(f.cfg.Concurrency)

	for i, file := range manifest.Files {
		g.Go(func() error {
			// Generate local filename
			localPath := filepath.Join(f.cfg.DownloadDir, sanitizeFilename(file.Key))

			// Download file
			if err := f.client.DownloadFile(ctx, manifest.DestinationBucket, file.Key, localPath); err != nil {
				return fmt.Errorf("download %s: %w", file.Key, err)
			}

			mu.Lock()
			localFiles[i] = localPath
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return localFiles, nil
}

// Cleanup removes downloaded files.
func (f *Fetcher) Cleanup() error {
	if f.cfg.KeepFiles {
		return nil
	}
	return os.RemoveAll(f.cfg.DownloadDir)
}

// sanitizeFilename converts an S3 key to a safe local filename.
func sanitizeFilename(key string) string {
	// Use the last component of the path
	base := filepath.Base(key)
	// Replace any remaining problematic characters
	base = strings.ReplaceAll(base, "/", "_")
	base = strings.ReplaceAll(base, "\\", "_")
	return base
}
