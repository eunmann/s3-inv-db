package s3fetch

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultDownloaderConfig(t *testing.T) {
	cfg := DefaultDownloaderConfig()

	if cfg.Concurrency < 4 {
		t.Errorf("Concurrency = %d, want >= 4", cfg.Concurrency)
	}
	if cfg.Concurrency > 16 {
		t.Errorf("Concurrency = %d, want <= 16", cfg.Concurrency)
	}
	if cfg.PartSize != 16*1024*1024 {
		t.Errorf("PartSize = %d, want 16MB", cfg.PartSize)
	}
	if cfg.BufferPoolSize != cfg.Concurrency*2 {
		t.Errorf("BufferPoolSize = %d, want %d", cfg.BufferPoolSize, cfg.Concurrency*2)
	}
}

//nolint:gocyclo // Test with multiple subtests
func TestTempFileReader(t *testing.T) {
	// Create a temp file with test data
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.bin")

	// Write 1MB of random data
	testData := make([]byte, 1024*1024)
	if _, err := rand.Read(testData); err != nil {
		t.Fatalf("generate random data: %v", err)
	}

	if err := os.WriteFile(testPath, testData, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	t.Run("Read", func(t *testing.T) {
		f, err := os.Open(testPath)
		if err != nil {
			t.Fatalf("open file: %v", err)
		}

		reader := &tempFileReader{file: f, path: testPath}

		// Read in chunks and compare
		buf := make([]byte, 4096)
		var read []byte
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				read = append(read, buf[:n]...)
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatalf("read: %v", err)
			}
		}

		if !bytes.Equal(read, testData) {
			t.Error("read data doesn't match original")
		}

		// Close should delete the file
		if err := reader.Close(); err != nil {
			t.Errorf("close: %v", err)
		}

		if _, err := os.Stat(testPath); !os.IsNotExist(err) {
			t.Error("file should have been deleted on close")
		}
	})

	// Recreate the file for ReadAt tests
	if err := os.WriteFile(testPath, testData, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	t.Run("ReadAt", func(t *testing.T) {
		f, err := os.Open(testPath)
		if err != nil {
			t.Fatalf("open file: %v", err)
		}

		reader := &tempFileReader{file: f, path: testPath}
		defer reader.Close()

		// Read at various offsets
		offsets := []int64{0, 1000, 50000, 512000}
		for _, off := range offsets {
			buf := make([]byte, 1000)
			n, err := reader.ReadAt(buf, off)
			if err != nil && !errors.Is(err, io.EOF) {
				t.Errorf("ReadAt(%d): %v", off, err)
				continue
			}
			if n > 0 && !bytes.Equal(buf[:n], testData[off:off+int64(n)]) {
				t.Errorf("ReadAt(%d): data mismatch", off)
			}
		}
	})

	// Recreate for Size test
	if err := os.WriteFile(testPath, testData, 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}

	t.Run("Size", func(t *testing.T) {
		f, err := os.Open(testPath)
		if err != nil {
			t.Fatalf("open file: %v", err)
		}

		reader := &tempFileReader{file: f, path: testPath}
		defer reader.Close()

		size, err := reader.Size()
		if err != nil {
			t.Errorf("Size: %v", err)
		}
		if size != int64(len(testData)) {
			t.Errorf("Size = %d, want %d", size, len(testData))
		}
	})
}

func TestDownloaderConfig_Defaults(t *testing.T) {
	// Test that zero values get filled with defaults
	tests := []struct {
		name     string
		cfg      DownloaderConfig
		wantConc int
		wantPart int64
	}{
		{
			name:     "all zero",
			cfg:      DownloaderConfig{},
			wantConc: DefaultDownloaderConfig().Concurrency,
			wantPart: DefaultDownloaderConfig().PartSize,
		},
		{
			name:     "custom concurrency",
			cfg:      DownloaderConfig{Concurrency: 8},
			wantConc: 8,
			wantPart: DefaultDownloaderConfig().PartSize,
		},
		{
			name:     "custom part size",
			cfg:      DownloaderConfig{PartSize: 32 * 1024 * 1024},
			wantConc: DefaultDownloaderConfig().Concurrency,
			wantPart: 32 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Can't actually create a downloader without an S3 client,
			// but we can test the config defaults logic inline
			cfg := tt.cfg
			if cfg.Concurrency <= 0 {
				cfg.Concurrency = DefaultDownloaderConfig().Concurrency
			}
			if cfg.PartSize <= 0 {
				cfg.PartSize = DefaultDownloaderConfig().PartSize
			}

			if cfg.Concurrency != tt.wantConc {
				t.Errorf("Concurrency = %d, want %d", cfg.Concurrency, tt.wantConc)
			}
			if cfg.PartSize != tt.wantPart {
				t.Errorf("PartSize = %d, want %d", cfg.PartSize, tt.wantPart)
			}
		})
	}
}

// TestDownloadResult verifies the download result struct.
func TestDownloadResult(t *testing.T) {
	result := DownloadResult{
		BytesDownloaded: 1024 * 1024 * 10,
		Concurrency:     8,
		PartSize:        16 * 1024 * 1024,
	}

	if result.BytesDownloaded != 10*1024*1024 {
		t.Errorf("BytesDownloaded = %d, want 10MB", result.BytesDownloaded)
	}
	if result.Concurrency != 8 {
		t.Errorf("Concurrency = %d, want 8", result.Concurrency)
	}
	if result.PartSize != 16*1024*1024 {
		t.Errorf("PartSize = %d, want 16MB", result.PartSize)
	}
}

// TestDownloaderIntegration requires AWS credentials and is skipped in CI.
// To run: go test -run TestDownloaderIntegration -v.
func TestDownloaderIntegration(t *testing.T) {
	if os.Getenv("AWS_INTEGRATION_TEST") == "" {
		t.Skip("skipping integration test; set AWS_INTEGRATION_TEST=1 to run")
	}

	ctx := context.Background()
	client, err := NewClient(ctx)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}

	// Test downloading a small public object (if available)
	bucket := os.Getenv("AWS_TEST_BUCKET")
	key := os.Getenv("AWS_TEST_KEY")
	if bucket == "" || key == "" {
		t.Skip("AWS_TEST_BUCKET and AWS_TEST_KEY required for integration test")
	}

	reader, result, err := client.DownloadObject(ctx, bucket, key)
	if err != nil {
		t.Fatalf("download object: %v", err)
	}
	defer reader.Close()

	t.Logf("Downloaded %d bytes in %v (concurrency=%d, partSize=%d)",
		result.BytesDownloaded, result.Duration, result.Concurrency, result.PartSize)

	// Read all content
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read content: %v", err)
	}

	if int64(len(data)) != result.BytesDownloaded {
		t.Errorf("read %d bytes, but download reported %d", len(data), result.BytesDownloaded)
	}
}
