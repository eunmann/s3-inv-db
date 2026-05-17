package s3fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// DownloaderConfig configures the S3 Download Manager.
type DownloaderConfig struct {
	// Concurrency is the number of concurrent download parts.
	// Default: max(4, NumCPU).
	Concurrency int

	// PartSize is the size of each download part in bytes.
	// Default: 16MB. Higher values use more memory but may improve throughput.
	PartSize int64

	// TempDir is the directory for temporary download files.
	// If empty, os.TempDir() is used.
	TempDir string
}

// DefaultDownloaderConfig returns sensible defaults based on the current machine.
func DefaultDownloaderConfig() DownloaderConfig {
	numCPU := runtime.NumCPU()
	concurrency := min(max(numCPU, 4), 16)

	return DownloaderConfig{
		Concurrency: concurrency,
		PartSize:    16 * 1024 * 1024, // 16MB
		TempDir:     "",
	}
}

// Downloader wraps the AWS S3 Transfer Manager for high-throughput downloads.
type Downloader struct {
	manager *transfermanager.Client
	config  DownloaderConfig
}

// NewDownloader creates an S3 Downloader from an existing S3 client.
func NewDownloader(s3Client *s3.Client, cfg DownloaderConfig) *Downloader {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultDownloaderConfig().Concurrency
	}
	if cfg.PartSize <= 0 {
		cfg.PartSize = DefaultDownloaderConfig().PartSize
	}

	mgr := transfermanager.New(s3Client, func(o *transfermanager.Options) {
		o.Concurrency = cfg.Concurrency
		o.PartSizeBytes = cfg.PartSize
	})

	return &Downloader{
		manager: mgr,
		config:  cfg,
	}
}

// DownloadResult contains information about a completed download.
type DownloadResult struct {
	// BytesDownloaded is the total bytes downloaded.
	BytesDownloaded int64

	// Duration is how long the download took.
	Duration time.Duration

	// Concurrency is the concurrency level used.
	Concurrency int

	// PartSize is the part size used.
	PartSize int64
}

// DownloadToReader downloads an S3 object and returns a streaming reader.
// The returned reader must be closed when done. The underlying temp file
// is automatically cleaned up on close.
//
// This method uses the AWS S3 Download Manager for parallel range downloads,
// which significantly improves throughput for large objects.
func (d *Downloader) DownloadToReader(ctx context.Context, bucket, key string) (*DownloadedObject, *DownloadResult, error) {
	startTime := time.Now()

	// Create temp file for download
	tempDir := d.config.TempDir
	if tempDir == "" {
		tempDir = os.TempDir()
	}

	tempFile, err := os.CreateTemp(tempDir, "s3download-*.tmp")
	if err != nil {
		return nil, nil, fmt.Errorf("create temp file: %w", err)
	}

	out, err := d.manager.DownloadObject(ctx, &transfermanager.DownloadObjectInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		WriterAt: tempFile,
	})
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, nil, fmt.Errorf("download s3://%s/%s: %w", bucket, key, err)
	}

	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())

		return nil, nil, fmt.Errorf("seek temp file: %w", err)
	}

	result := &DownloadResult{
		BytesDownloaded: bytesDownloaded(out, tempFile),
		Duration:        time.Since(startTime),
		Concurrency:     d.config.Concurrency,
		PartSize:        d.config.PartSize,
	}

	return &DownloadedObject{
		file: tempFile,
		path: tempFile.Name(),
	}, result, nil
}

// bytesDownloaded returns the size written for a DownloadObject call.
// Prefers the service-reported ContentLength; falls back to stat'ing the
// destination file when the field is absent.
func bytesDownloaded(out *transfermanager.DownloadObjectOutput, f *os.File) int64 {
	if out != nil && out.ContentLength != nil {
		return *out.ContentLength
	}
	if info, err := f.Stat(); err == nil {
		return info.Size()
	}

	return 0
}

// Config returns the downloader configuration.
func (d *Downloader) Config() DownloaderConfig {
	return d.config
}

// DownloadedObject is the random-access reader returned by
// Client.DownloadObject / Downloader.DownloadToReader. Backed by a
// temp file that is removed on Close. Supports streaming (Read),
// random access (ReadAt), and length (Size) so callers can choose the
// access pattern that fits their parser (e.g. Parquet needs ReadAt).
type DownloadedObject struct {
	file *os.File
	path string
}

func (d *DownloadedObject) Read(p []byte) (int, error) {
	n, err := d.file.Read(p)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, io.EOF
		}

		return n, fmt.Errorf("read temp file: %w", err)
	}

	return n, nil
}

func (d *DownloadedObject) Close() error {
	err := d.file.Close()
	os.Remove(d.path)
	if err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	return nil
}

func (d *DownloadedObject) ReadAt(p []byte, off int64) (int, error) {
	n, err := d.file.ReadAt(p, off)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, io.EOF
		}

		return n, fmt.Errorf("read temp file at offset %d: %w", off, err)
	}

	return n, nil
}

// Size returns the downloaded byte count.
func (d *DownloadedObject) Size() (int64, error) {
	info, err := d.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat temp file: %w", err)
	}

	return info.Size(), nil
}
