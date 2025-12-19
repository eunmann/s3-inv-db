package sqliteagg

import (
	"runtime"
)

// BuildOptions controls concurrency and performance settings for the build pipeline.
type BuildOptions struct {
	// S3DownloadConcurrency is the number of parallel S3 chunk downloads.
	// Default: min(runtime.NumCPU(), 16)
	S3DownloadConcurrency int

	// ParseWorkers is the number of goroutines for CSV parsing and prefix expansion.
	// Default: runtime.NumCPU()
	ParseWorkers int

	// SQLiteWriteBatchSize is the number of prefix updates to batch per transaction.
	// Larger values improve throughput but increase memory usage.
	// Default: 50000
	SQLiteWriteBatchSize int

	// SQLiteWriteBatchTimeoutMs is the maximum time in milliseconds to wait before
	// flushing a partial batch. Default: 200
	SQLiteWriteBatchTimeoutMs int

	// RowChannelBuffer is the buffer size for the parsed row channel.
	// Default: 100000
	RowChannelBuffer int

	// DeltaChannelBuffer is the buffer size for the prefix delta channel.
	// Default: 500000
	DeltaChannelBuffer int

	// FileWriteConcurrency is the number of parallel file writes during index build.
	// Default: 4
	FileWriteConcurrency int
}

// DefaultBuildOptions returns sensible default build options.
func DefaultBuildOptions() BuildOptions {
	numCPU := runtime.NumCPU()
	s3Concurrency := numCPU
	if s3Concurrency > 16 {
		s3Concurrency = 16
	}
	if s3Concurrency < 4 {
		s3Concurrency = 4
	}

	return BuildOptions{
		S3DownloadConcurrency:     s3Concurrency,
		ParseWorkers:              numCPU,
		SQLiteWriteBatchSize:      50000,
		SQLiteWriteBatchTimeoutMs: 200,
		RowChannelBuffer:          100000,
		DeltaChannelBuffer:        500000,
		FileWriteConcurrency:      4,
	}
}

// Validate checks that all options are within valid ranges and sets defaults for zero values.
func (o *BuildOptions) Validate() {
	if o.S3DownloadConcurrency <= 0 {
		o.S3DownloadConcurrency = DefaultBuildOptions().S3DownloadConcurrency
	}
	if o.ParseWorkers <= 0 {
		o.ParseWorkers = DefaultBuildOptions().ParseWorkers
	}
	if o.SQLiteWriteBatchSize <= 0 {
		o.SQLiteWriteBatchSize = DefaultBuildOptions().SQLiteWriteBatchSize
	}
	if o.SQLiteWriteBatchTimeoutMs <= 0 {
		o.SQLiteWriteBatchTimeoutMs = DefaultBuildOptions().SQLiteWriteBatchTimeoutMs
	}
	if o.RowChannelBuffer <= 0 {
		o.RowChannelBuffer = DefaultBuildOptions().RowChannelBuffer
	}
	if o.DeltaChannelBuffer <= 0 {
		o.DeltaChannelBuffer = DefaultBuildOptions().DeltaChannelBuffer
	}
	if o.FileWriteConcurrency <= 0 {
		o.FileWriteConcurrency = DefaultBuildOptions().FileWriteConcurrency
	}
}

// WithS3DownloadConcurrency sets the S3 download concurrency.
func (o BuildOptions) WithS3DownloadConcurrency(n int) BuildOptions {
	o.S3DownloadConcurrency = n
	return o
}

// WithParseWorkers sets the number of parse workers.
func (o BuildOptions) WithParseWorkers(n int) BuildOptions {
	o.ParseWorkers = n
	return o
}

// WithSQLiteWriteBatchSize sets the SQLite write batch size.
func (o BuildOptions) WithSQLiteWriteBatchSize(n int) BuildOptions {
	o.SQLiteWriteBatchSize = n
	return o
}

// WithSQLiteWriteBatchTimeoutMs sets the SQLite write batch timeout.
func (o BuildOptions) WithSQLiteWriteBatchTimeoutMs(ms int) BuildOptions {
	o.SQLiteWriteBatchTimeoutMs = ms
	return o
}

// WithRowChannelBuffer sets the row channel buffer size.
func (o BuildOptions) WithRowChannelBuffer(n int) BuildOptions {
	o.RowChannelBuffer = n
	return o
}

// WithDeltaChannelBuffer sets the delta channel buffer size.
func (o BuildOptions) WithDeltaChannelBuffer(n int) BuildOptions {
	o.DeltaChannelBuffer = n
	return o
}

// WithFileWriteConcurrency sets the file write concurrency.
func (o BuildOptions) WithFileWriteConcurrency(n int) BuildOptions {
	o.FileWriteConcurrency = n
	return o
}

// TestBuildOptions returns minimal options suitable for testing.
func TestBuildOptions() BuildOptions {
	return BuildOptions{
		S3DownloadConcurrency:     2,
		ParseWorkers:              2,
		SQLiteWriteBatchSize:      1000,
		SQLiteWriteBatchTimeoutMs: 50,
		RowChannelBuffer:          1000,
		DeltaChannelBuffer:        5000,
		FileWriteConcurrency:      2,
	}
}
