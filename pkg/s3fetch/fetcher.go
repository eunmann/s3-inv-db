package s3fetch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	// StorageClassColumn is the index of the StorageClass column (-1 if absent).
	StorageClassColumn int
	// AccessTierColumn is the index of the IntelligentTieringAccessTier column (-1 if absent).
	AccessTierColumn int
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
		return nil, fmt.Errorf("get key column index: %w", err)
	}
	sizeCol, err := manifest.SizeColumnIndex()
	if err != nil {
		return nil, fmt.Errorf("get size column index: %w", err)
	}

	// Create download directory
	if err := os.MkdirAll(f.cfg.DownloadDir, 0755); err != nil {
		return nil, fmt.Errorf("create download dir: %w", err)
	}

	// Get optional tier column indices
	storageCol := manifest.StorageClassColumnIndex()
	accessTierCol := manifest.AccessTierColumnIndex()

	// Download all inventory files concurrently
	localFiles, err := f.downloadFiles(ctx, manifest)
	if err != nil {
		return nil, fmt.Errorf("download inventory files: %w", err)
	}

	return &FetchResult{
		Manifest:           manifest,
		LocalFiles:         localFiles,
		KeyColumn:          keyCol,
		SizeColumn:         sizeCol,
		StorageClassColumn: storageCol,
		AccessTierColumn:   accessTierCol,
	}, nil
}

func (f *Fetcher) downloadFiles(ctx context.Context, manifest *Manifest) ([]string, error) {
	// Parse the destination bucket - it may be a plain bucket name or an ARN
	bucketName, err := manifest.GetDestinationBucketName()
	if err != nil {
		return nil, fmt.Errorf("parse destination bucket %q: %w", manifest.DestinationBucket, err)
	}

	localFiles := make([]string, len(manifest.Files))
	var mu sync.Mutex

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(f.cfg.Concurrency)

	for i, file := range manifest.Files {
		g.Go(func() error {
			// Generate local filename
			localPath := filepath.Join(f.cfg.DownloadDir, sanitizeFilename(file.Key))

			// Download file using the normalized bucket name
			if err := f.client.DownloadFile(ctx, bucketName, file.Key, localPath); err != nil {
				return fmt.Errorf("download %s: %w", file.Key, err)
			}

			mu.Lock()
			localFiles[i] = localPath
			mu.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("wait for downloads: %w", err)
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
	// filepath.Base extracts the final path component, removing all directory separators
	return filepath.Base(key)
}
