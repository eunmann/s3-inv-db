package sqliteagg

import (
	"testing"
)

func TestBuildOptionsValidate_ZeroValues(t *testing.T) {
	// All zero values should get defaults
	opts := BuildOptions{}
	opts.Validate()

	defaults := DefaultBuildOptions()

	if opts.S3DownloadConcurrency != defaults.S3DownloadConcurrency {
		t.Errorf("S3DownloadConcurrency = %d, want %d", opts.S3DownloadConcurrency, defaults.S3DownloadConcurrency)
	}
	if opts.ParseWorkers != defaults.ParseWorkers {
		t.Errorf("ParseWorkers = %d, want %d", opts.ParseWorkers, defaults.ParseWorkers)
	}
	if opts.SQLiteWriteBatchSize != defaults.SQLiteWriteBatchSize {
		t.Errorf("SQLiteWriteBatchSize = %d, want %d", opts.SQLiteWriteBatchSize, defaults.SQLiteWriteBatchSize)
	}
	if opts.SQLiteWriteBatchTimeoutMs != defaults.SQLiteWriteBatchTimeoutMs {
		t.Errorf("SQLiteWriteBatchTimeoutMs = %d, want %d", opts.SQLiteWriteBatchTimeoutMs, defaults.SQLiteWriteBatchTimeoutMs)
	}
	if opts.RowChannelBuffer != defaults.RowChannelBuffer {
		t.Errorf("RowChannelBuffer = %d, want %d", opts.RowChannelBuffer, defaults.RowChannelBuffer)
	}
	if opts.DeltaChannelBuffer != defaults.DeltaChannelBuffer {
		t.Errorf("DeltaChannelBuffer = %d, want %d", opts.DeltaChannelBuffer, defaults.DeltaChannelBuffer)
	}
	if opts.FileWriteConcurrency != defaults.FileWriteConcurrency {
		t.Errorf("FileWriteConcurrency = %d, want %d", opts.FileWriteConcurrency, defaults.FileWriteConcurrency)
	}
}

func TestBuildOptionsValidate_NegativeValues(t *testing.T) {
	// Negative values should get defaults
	opts := BuildOptions{
		S3DownloadConcurrency:     -1,
		ParseWorkers:              -5,
		SQLiteWriteBatchSize:      -100,
		SQLiteWriteBatchTimeoutMs: -200,
		RowChannelBuffer:          -1000,
		DeltaChannelBuffer:        -5000,
		FileWriteConcurrency:      -2,
	}
	opts.Validate()

	defaults := DefaultBuildOptions()

	if opts.S3DownloadConcurrency != defaults.S3DownloadConcurrency {
		t.Errorf("S3DownloadConcurrency = %d after negative input, want %d", opts.S3DownloadConcurrency, defaults.S3DownloadConcurrency)
	}
	if opts.ParseWorkers != defaults.ParseWorkers {
		t.Errorf("ParseWorkers = %d after negative input, want %d", opts.ParseWorkers, defaults.ParseWorkers)
	}
	if opts.SQLiteWriteBatchSize != defaults.SQLiteWriteBatchSize {
		t.Errorf("SQLiteWriteBatchSize = %d after negative input, want %d", opts.SQLiteWriteBatchSize, defaults.SQLiteWriteBatchSize)
	}
	if opts.SQLiteWriteBatchTimeoutMs != defaults.SQLiteWriteBatchTimeoutMs {
		t.Errorf("SQLiteWriteBatchTimeoutMs = %d after negative input, want %d", opts.SQLiteWriteBatchTimeoutMs, defaults.SQLiteWriteBatchTimeoutMs)
	}
	if opts.RowChannelBuffer != defaults.RowChannelBuffer {
		t.Errorf("RowChannelBuffer = %d after negative input, want %d", opts.RowChannelBuffer, defaults.RowChannelBuffer)
	}
	if opts.DeltaChannelBuffer != defaults.DeltaChannelBuffer {
		t.Errorf("DeltaChannelBuffer = %d after negative input, want %d", opts.DeltaChannelBuffer, defaults.DeltaChannelBuffer)
	}
	if opts.FileWriteConcurrency != defaults.FileWriteConcurrency {
		t.Errorf("FileWriteConcurrency = %d after negative input, want %d", opts.FileWriteConcurrency, defaults.FileWriteConcurrency)
	}
}

func TestBuildOptionsValidate_PositiveValues(t *testing.T) {
	// Positive values should be preserved
	opts := BuildOptions{
		S3DownloadConcurrency:     8,
		ParseWorkers:              4,
		SQLiteWriteBatchSize:      10000,
		SQLiteWriteBatchTimeoutMs: 100,
		RowChannelBuffer:          50000,
		DeltaChannelBuffer:        100000,
		FileWriteConcurrency:      2,
	}
	opts.Validate()

	if opts.S3DownloadConcurrency != 8 {
		t.Errorf("S3DownloadConcurrency = %d, want 8", opts.S3DownloadConcurrency)
	}
	if opts.ParseWorkers != 4 {
		t.Errorf("ParseWorkers = %d, want 4", opts.ParseWorkers)
	}
	if opts.SQLiteWriteBatchSize != 10000 {
		t.Errorf("SQLiteWriteBatchSize = %d, want 10000", opts.SQLiteWriteBatchSize)
	}
	if opts.SQLiteWriteBatchTimeoutMs != 100 {
		t.Errorf("SQLiteWriteBatchTimeoutMs = %d, want 100", opts.SQLiteWriteBatchTimeoutMs)
	}
	if opts.RowChannelBuffer != 50000 {
		t.Errorf("RowChannelBuffer = %d, want 50000", opts.RowChannelBuffer)
	}
	if opts.DeltaChannelBuffer != 100000 {
		t.Errorf("DeltaChannelBuffer = %d, want 100000", opts.DeltaChannelBuffer)
	}
	if opts.FileWriteConcurrency != 2 {
		t.Errorf("FileWriteConcurrency = %d, want 2", opts.FileWriteConcurrency)
	}
}

func TestBuildOptionsValidate_PartialZero(t *testing.T) {
	// Only some values zero - only those should get defaults
	defaults := DefaultBuildOptions()
	opts := BuildOptions{
		S3DownloadConcurrency: 10,
		ParseWorkers:          0, // Should get default
		SQLiteWriteBatchSize:  5000,
	}
	opts.Validate()

	if opts.S3DownloadConcurrency != 10 {
		t.Errorf("S3DownloadConcurrency = %d, want 10", opts.S3DownloadConcurrency)
	}
	if opts.ParseWorkers != defaults.ParseWorkers {
		t.Errorf("ParseWorkers = %d, want %d (default)", opts.ParseWorkers, defaults.ParseWorkers)
	}
	if opts.SQLiteWriteBatchSize != 5000 {
		t.Errorf("SQLiteWriteBatchSize = %d, want 5000", opts.SQLiteWriteBatchSize)
	}
}

func TestDefaultBuildOptions(t *testing.T) {
	opts := DefaultBuildOptions()

	// All fields should have positive values
	if opts.S3DownloadConcurrency <= 0 {
		t.Errorf("S3DownloadConcurrency = %d, want > 0", opts.S3DownloadConcurrency)
	}
	if opts.ParseWorkers <= 0 {
		t.Errorf("ParseWorkers = %d, want > 0", opts.ParseWorkers)
	}
	if opts.SQLiteWriteBatchSize <= 0 {
		t.Errorf("SQLiteWriteBatchSize = %d, want > 0", opts.SQLiteWriteBatchSize)
	}
	if opts.SQLiteWriteBatchTimeoutMs <= 0 {
		t.Errorf("SQLiteWriteBatchTimeoutMs = %d, want > 0", opts.SQLiteWriteBatchTimeoutMs)
	}
	if opts.RowChannelBuffer <= 0 {
		t.Errorf("RowChannelBuffer = %d, want > 0", opts.RowChannelBuffer)
	}
	if opts.DeltaChannelBuffer <= 0 {
		t.Errorf("DeltaChannelBuffer = %d, want > 0", opts.DeltaChannelBuffer)
	}
	if opts.FileWriteConcurrency <= 0 {
		t.Errorf("FileWriteConcurrency = %d, want > 0", opts.FileWriteConcurrency)
	}

	// S3DownloadConcurrency should be capped
	if opts.S3DownloadConcurrency > 16 {
		t.Errorf("S3DownloadConcurrency = %d, want <= 16", opts.S3DownloadConcurrency)
	}
	if opts.S3DownloadConcurrency < 4 {
		t.Errorf("S3DownloadConcurrency = %d, want >= 4", opts.S3DownloadConcurrency)
	}
}

func TestBuildOptionsChaining(t *testing.T) {
	opts := DefaultBuildOptions().
		WithS3DownloadConcurrency(12).
		WithParseWorkers(8).
		WithSQLiteWriteBatchSize(25000).
		WithSQLiteWriteBatchTimeoutMs(150).
		WithRowChannelBuffer(75000).
		WithDeltaChannelBuffer(250000).
		WithFileWriteConcurrency(6)

	if opts.S3DownloadConcurrency != 12 {
		t.Errorf("S3DownloadConcurrency = %d, want 12", opts.S3DownloadConcurrency)
	}
	if opts.ParseWorkers != 8 {
		t.Errorf("ParseWorkers = %d, want 8", opts.ParseWorkers)
	}
	if opts.SQLiteWriteBatchSize != 25000 {
		t.Errorf("SQLiteWriteBatchSize = %d, want 25000", opts.SQLiteWriteBatchSize)
	}
	if opts.SQLiteWriteBatchTimeoutMs != 150 {
		t.Errorf("SQLiteWriteBatchTimeoutMs = %d, want 150", opts.SQLiteWriteBatchTimeoutMs)
	}
	if opts.RowChannelBuffer != 75000 {
		t.Errorf("RowChannelBuffer = %d, want 75000", opts.RowChannelBuffer)
	}
	if opts.DeltaChannelBuffer != 250000 {
		t.Errorf("DeltaChannelBuffer = %d, want 250000", opts.DeltaChannelBuffer)
	}
	if opts.FileWriteConcurrency != 6 {
		t.Errorf("FileWriteConcurrency = %d, want 6", opts.FileWriteConcurrency)
	}
}

func TestTestBuildOptions(t *testing.T) {
	opts := TestBuildOptions()

	// Test options should have smaller values for faster tests
	if opts.S3DownloadConcurrency > 4 {
		t.Errorf("Test S3DownloadConcurrency = %d, want <= 4", opts.S3DownloadConcurrency)
	}
	if opts.SQLiteWriteBatchSize > 10000 {
		t.Errorf("Test SQLiteWriteBatchSize = %d, want <= 10000", opts.SQLiteWriteBatchSize)
	}
}
