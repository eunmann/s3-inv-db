package s3fetch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/fileutil"
	"github.com/eunmann/s3-inv-db/pkg/logging"
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
	log := logging.WithPhase("fetch_manifest")

	// Parse manifest URI
	bucket, key, err := ParseS3URI(f.cfg.ManifestURI)
	if err != nil {
		return nil, fmt.Errorf("parse manifest URI: %w", err)
	}

	log.Info().
		Str("s3_manifest", f.cfg.ManifestURI).
		Str("bucket", bucket).
		Str("key", key).
		Msg("fetching manifest")

	fetchStart := time.Now()

	// Fetch and parse manifest
	manifest, err := f.client.FetchManifest(ctx, bucket, key)
	if err != nil {
		log.Error().Err(err).Str("bucket", bucket).Str("key", key).Msg("failed to fetch manifest")
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}

	log.Info().
		Int("file_count", len(manifest.Files)).
		Str("source_bucket", manifest.SourceBucket).
		Str("file_format", manifest.FileFormat).
		Dur("elapsed", time.Since(fetchStart)).
		Msg("manifest fetched successfully")

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
	log := logging.WithPhase("download_inventory")

	// Parse the destination bucket - it may be a plain bucket name or an ARN
	bucketName, err := manifest.GetDestinationBucketName()
	if err != nil {
		return nil, fmt.Errorf("parse destination bucket %q: %w", manifest.DestinationBucket, err)
	}

	totalFiles := len(manifest.Files)
	var totalExpectedBytes int64
	for _, f := range manifest.Files {
		totalExpectedBytes += f.Size
	}

	// Clean up any stale .tmp files from previous interrupted downloads
	if err := fileutil.CleanupTmpFiles(f.cfg.DownloadDir); err != nil {
		log.Warn().Err(err).Msg("failed to cleanup tmp files")
	}

	log.Info().
		Int("file_count", totalFiles).
		Int64("total_bytes", totalExpectedBytes).
		Str("bucket", bucketName).
		Int("concurrency", f.cfg.Concurrency).
		Msg("starting inventory download")

	downloadStart := time.Now()
	var filesDownloaded atomic.Int64
	var filesSkipped atomic.Int64
	var bytesDownloaded atomic.Int64

	localFiles := make([]string, len(manifest.Files))
	var mu sync.Mutex

	// Progress logging goroutine
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				files := filesDownloaded.Load()
				skipped := filesSkipped.Load()
				bytes := bytesDownloaded.Load()
				log.Info().
					Int64("files_downloaded", files).
					Int64("files_skipped", skipped).
					Int("files_total", totalFiles).
					Int64("bytes_downloaded", bytes).
					Int64("bytes_total", totalExpectedBytes).
					Dur("elapsed", time.Since(downloadStart)).
					Msg("download progress")
			case <-progressDone:
				return
			}
		}
	}()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(f.cfg.Concurrency)

	for i, file := range manifest.Files {
		fileSize := file.Size
		g.Go(func() error {
			// Generate local filename
			localPath := filepath.Join(f.cfg.DownloadDir, sanitizeFilename(file.Key))

			// Skip if file already exists and is non-empty (resume support)
			if fileutil.IsNonEmpty(localPath) {
				filesSkipped.Add(1)
				log.Debug().
					Str("key", file.Key).
					Msg("skipping already downloaded file")

				mu.Lock()
				localFiles[i] = localPath
				mu.Unlock()
				return nil
			}

			// Download file using the normalized bucket name
			if err := f.client.DownloadFile(ctx, bucketName, file.Key, localPath); err != nil {
				log.Error().
					Err(err).
					Str("bucket", bucketName).
					Str("key", file.Key).
					Msg("failed to download inventory file")
				return fmt.Errorf("download %s: %w", file.Key, err)
			}

			filesDownloaded.Add(1)
			bytesDownloaded.Add(fileSize)

			log.Debug().
				Str("key", file.Key).
				Int64("size", fileSize).
				Msg("downloaded file")

			mu.Lock()
			localFiles[i] = localPath
			mu.Unlock()

			return nil
		})
	}

	err = g.Wait()
	close(progressDone)

	if err != nil {
		return nil, fmt.Errorf("wait for downloads: %w", err)
	}

	log.Info().
		Int64("files_downloaded", filesDownloaded.Load()).
		Int64("files_skipped", filesSkipped.Load()).
		Int64("bytes_downloaded", bytesDownloaded.Load()).
		Dur("elapsed", time.Since(downloadStart)).
		Msg("inventory download complete")

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
