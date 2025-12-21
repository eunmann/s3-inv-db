package s3fetch

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
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

	// BufferPoolSize is the number of pre-allocated buffers in the pool.
	// Default: Concurrency * 2.
	BufferPoolSize int
}

// DefaultDownloaderConfig returns sensible defaults based on the current machine.
func DefaultDownloaderConfig() DownloaderConfig {
	numCPU := runtime.NumCPU()
	concurrency := numCPU
	if concurrency < 4 {
		concurrency = 4
	}
	if concurrency > 16 {
		concurrency = 16
	}

	return DownloaderConfig{
		Concurrency:    concurrency,
		PartSize:       16 * 1024 * 1024, // 16MB
		TempDir:        "",
		BufferPoolSize: concurrency * 2,
	}
}

// Downloader wraps the AWS S3 Download Manager for high-throughput downloads.
type Downloader struct {
	manager    *manager.Downloader
	config     DownloaderConfig
	bufferPool *sync.Pool
}

// NewDownloader creates an S3 Downloader from an existing S3 client.
func NewDownloader(s3Client *s3.Client, cfg DownloaderConfig) *Downloader {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = DefaultDownloaderConfig().Concurrency
	}
	if cfg.PartSize <= 0 {
		cfg.PartSize = DefaultDownloaderConfig().PartSize
	}
	if cfg.BufferPoolSize <= 0 {
		cfg.BufferPoolSize = cfg.Concurrency * 2
	}

	// Create the AWS Download Manager with configured options
	mgr := manager.NewDownloader(s3Client, func(d *manager.Downloader) {
		d.Concurrency = cfg.Concurrency
		d.PartSize = cfg.PartSize
		// Use a buffer pool to reduce allocations
		d.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(int(cfg.PartSize))
	})

	// Create a buffer pool for temporary files
	bufferPool := &sync.Pool{
		New: func() any {
			return make([]byte, 32*1024) // 32KB read buffer
		},
	}

	return &Downloader{
		manager:    mgr,
		config:     cfg,
		bufferPool: bufferPool,
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
func (d *Downloader) DownloadToReader(ctx context.Context, bucket, key string) (io.ReadCloser, *DownloadResult, error) {
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

	// Download to the temp file using the download manager
	n, err := d.manager.Download(ctx, tempFile, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, nil, fmt.Errorf("download s3://%s/%s: %w", bucket, key, err)
	}

	// Seek back to start for reading
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, nil, fmt.Errorf("seek temp file: %w", err)
	}

	result := &DownloadResult{
		BytesDownloaded: n,
		Duration:        time.Since(startTime),
		Concurrency:     d.config.Concurrency,
		PartSize:        d.config.PartSize,
	}

	// Wrap in a reader that cleans up the temp file on close
	reader := &tempFileReader{
		file: tempFile,
		path: tempFile.Name(),
	}

	return reader, result, nil
}

// DownloadToFile downloads an S3 object to a specified file path.
// Returns download statistics.
func (d *Downloader) DownloadToFile(ctx context.Context, bucket, key, destPath string) (*DownloadResult, error) {
	startTime := time.Now()

	file, err := os.Create(destPath)
	if err != nil {
		return nil, fmt.Errorf("create destination file: %w", err)
	}
	defer file.Close()

	n, err := d.manager.Download(ctx, file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		os.Remove(destPath)
		return nil, fmt.Errorf("download s3://%s/%s: %w", bucket, key, err)
	}

	return &DownloadResult{
		BytesDownloaded: n,
		Duration:        time.Since(startTime),
		Concurrency:     d.config.Concurrency,
		PartSize:        d.config.PartSize,
	}, nil
}

// Config returns the downloader configuration.
func (d *Downloader) Config() DownloaderConfig {
	return d.config
}

// tempFileReader wraps an os.File and deletes it on close.
type tempFileReader struct {
	file *os.File
	path string
}

func (r *tempFileReader) Read(p []byte) (n int, err error) {
	n, err = r.file.Read(p)
	if err != nil {
		if err == io.EOF {
			return n, io.EOF
		}
		return n, fmt.Errorf("read temp file: %w", err)
	}
	return n, nil
}

func (r *tempFileReader) Close() error {
	err := r.file.Close()
	os.Remove(r.path)
	if err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	return nil
}

// ReadAt implements io.ReaderAt for Parquet compatibility.
func (r *tempFileReader) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = r.file.ReadAt(p, off)
	if err != nil {
		if err == io.EOF {
			return n, io.EOF
		}
		return n, fmt.Errorf("read temp file at offset %d: %w", off, err)
	}
	return n, nil
}

// Size returns the file size for Parquet compatibility.
func (r *tempFileReader) Size() (int64, error) {
	info, err := r.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("stat temp file: %w", err)
	}
	return info.Size(), nil
}
