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
	s3Client *s3.Client
}

// NewClient creates a new S3 client using default AWS configuration.
func NewClient(ctx context.Context) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	return &Client{
		s3Client: s3.NewFromConfig(cfg),
	}, nil
}

// NewClientWithConfig creates a new S3 client with a custom AWS config.
func NewClientWithConfig(cfg aws.Config) *Client {
	return &Client{
		s3Client: s3.NewFromConfig(cfg),
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

// StreamObject returns a reader for an S3 object.
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
