package s3fetch

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Client provides S3 operations for fetching inventory files.
type Client struct {
	s3Client   *s3.Client
	downloader *Downloader
}

// NewClient creates a new S3 client using default AWS configuration.
func NewClient(ctx context.Context) (*Client, error) {
	return NewClientWithDownloaderConfig(ctx, DefaultDownloaderConfig())
}

// NewClientWithDownloaderConfig creates a new S3 client with custom downloader configuration.
func NewClientWithDownloaderConfig(ctx context.Context, dlCfg DownloaderConfig) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(cfg)
	return &Client{
		s3Client:   s3Client,
		downloader: NewDownloader(s3Client, dlCfg),
	}, nil
}

// NewClientWithConfig creates a new S3 client with a custom AWS config.
func NewClientWithConfig(cfg aws.Config) *Client {
	return NewClientWithConfigAndDownloader(cfg, DefaultDownloaderConfig())
}

// NewClientWithConfigAndDownloader creates a new S3 client with custom AWS and downloader configs.
func NewClientWithConfigAndDownloader(cfg aws.Config, dlCfg DownloaderConfig) *Client {
	s3Client := s3.NewFromConfig(cfg)
	return &Client{
		s3Client:   s3Client,
		downloader: NewDownloader(s3Client, dlCfg),
	}
}

// FetchManifest fetches and parses an S3 inventory manifest.
func (c *Client) FetchManifest(ctx context.Context, bucket, key string) (*Manifest, error) {
	resp, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get manifest from s3://%s/%s: %w", bucket, key, err)
	}
	defer resp.Body.Close()

	manifest, err := ParseManifest(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse manifest from s3://%s/%s: %w", bucket, key, err)
	}
	return manifest, nil
}

// StreamObject returns a reader for an S3 object using a simple GetObject call.
//
// Deprecated: Use DownloadObject for better throughput on large objects.
// StreamObject is kept for backwards compatibility and small objects like manifests.
func (c *Client) StreamObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	resp, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object s3://%s/%s: %w", bucket, key, err)
	}
	return resp.Body, nil
}

// DownloadObject downloads an S3 object using the S3 Download Manager for parallel
// range downloads. This provides significantly better throughput for large objects.
// Returns a reader for the downloaded content and download statistics.
//
// The returned reader must be closed when done. The underlying temp file is
// automatically cleaned up on close.
func (c *Client) DownloadObject(ctx context.Context, bucket, key string) (io.ReadCloser, *DownloadResult, error) {
	return c.downloader.DownloadToReader(ctx, bucket, key)
}

// DownloaderConfig returns the current downloader configuration.
func (c *Client) DownloaderConfig() DownloaderConfig {
	return c.downloader.Config()
}
