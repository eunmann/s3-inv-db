package loader_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/internal/testsupport/miniotest"
	"github.com/rs/zerolog"
)

// BenchmarkPipeline_MultiChunkBuild runs the full build pipeline against
// a MinIO-hosted multi-chunk inventory. The chunk count is the key
// dimension: it gates how much chunk-level parallelism the pipeline can
// actually use, so changes to worker-count derivation surface here in
// a way they don't in any pkg/extsort microbench.
//
// Requires the docker compose `test` profile (AWS_ENDPOINT_URL_S3 must
// point at a reachable MinIO). Runs under `make test` paths but isn't
// part of the standard test run — invoke explicitly:
//
//	go test -bench=BenchmarkPipeline_MultiChunkBuild -benchtime=1x \
//	  -run=^$ -count=N ./internal/loader/...
func BenchmarkPipeline_MultiChunkBuild(b *testing.B) {
	cases := []struct {
		name    string
		objects int
		chunks  int
	}{
		{"objects=20000_chunks=4", 20000, 4},
		{"objects=20000_chunks=16", 20000, 16},
		{"objects=100000_chunks=8", 100000, 8},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			benchmarkMultiChunkBuild(b, c.objects, c.chunks)
		})
	}
}

func benchmarkMultiChunkBuild(b *testing.B, numObjects, numChunks int) {
	b.Helper()
	b.ReportAllocs()

	// The pipeline's internal debug logging would otherwise drown
	// the bench output line and confuse benchstat parsing.
	prev := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.Disabled)
	b.Cleanup(func() { zerolog.SetGlobalLevel(prev) })

	fc := miniotest.FetchClient(b)
	bucket := miniotest.Bucket(b, fc.Raw())

	srcBucket := "bench-src"
	prefix := "bench-inv/"
	stamp := time.Now().UTC().Truncate(time.Minute)

	info, err := seeder.UploadMultiChunkInventory(
		b.Context(), fc.Raw(),
		seeder.Config{
			Target:  seeder.TargetS3,
			Objects: numObjects,
			Preset:  "small",
			Seed:    int64(numObjects ^ numChunks),
			Logger:  zerolog.Nop(),
		},
		seeder.S3Config{
			Bucket:    bucket,
			Prefix:    prefix,
			SrcBucket: srcBucket,
		},
		1, int64(numObjects), stamp, numChunks,
	)
	if err != nil {
		b.Fatalf("UploadMultiChunkInventory: %v", err)
	}

	run := stamp.Format("2006-01-02T15-04Z")
	b.ResetTimer()
	for range b.N {
		// Use a fresh cache dir per iteration so each run rebuilds from
		// scratch; the loader RemoveAlls cache_dir/<src>/<inv>/<run>
		// before each build but a fresh root is even cleaner.
		cacheRoot := b.TempDir()
		l := loader.New(cacheRoot, fc)
		if _, err := l.BuildWith(b.Context(), srcBucket, info.ID, run, info.Path, nilProgress); err != nil {
			b.Fatalf("BuildWith: %v", err)
		}
	}
	_ = fmt.Sprintf
}

func nilProgress(string, int64, int64) {}
