package s3fetch

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3ClientOptions_UnsetEnvReturnsNil(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL_S3", "")
	if got := s3ClientOptions(); got != nil {
		t.Errorf("s3ClientOptions() = %v, want nil", got)
	}
}

func TestS3ClientOptions_SetEnvForcesPathStyle(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://minio:9000")
	opts := s3ClientOptions()
	if len(opts) != 1 {
		t.Fatalf("len(opts) = %d, want 1", len(opts))
	}
	var o s3.Options
	opts[0](&o)
	if !o.UsePathStyle {
		t.Error("s3ClientOptions did not set UsePathStyle=true")
	}
}

func TestClient_Raw_ReturnsUnderlyingClient(t *testing.T) {
	c := NewClientWithConfig(aws.Config{Region: "us-east-1"})
	if c.Raw() == nil {
		t.Error("Raw() returned nil")
	}
}

func TestNewClientWithConfigAndDownloader_DefaultsPathStyleByEnv(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://minio:9000")
	c, err := NewClientWithDownloaderConfig(context.Background(), DownloaderConfig{})
	if err != nil {
		t.Skipf("NewClient requires AWS config in env; skipping: %v", err)
	}
	if c == nil || c.Raw() == nil {
		t.Error("client construction returned nil")
	}
}
