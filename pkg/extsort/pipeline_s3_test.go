//go:build integration

package extsort_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

type seeded struct {
	client   *s3fetch.Client
	manifest string // s3:// URI
}

// seedManifest uploads a small synthetic inventory.
func seedManifest(t *testing.T) seeded {
	t.Helper()
	fc := s3test.FetchClient(t)
	bucket := s3test.Bucket(t, fc.Raw())
	info, err := seeder.UploadInventory(t.Context(), fc.Raw(), seeder.Config{
		Target:  seeder.TargetS3,
		Objects: 500,
		Preset:  "small",
		Seed:    7,
		Logger:  zerolog.Nop(),
	}, seeder.S3Config{Bucket: bucket, Prefix: "inv/", SrcBucket: "src"}, 1, 7, time.Now().UTC().Truncate(time.Minute))
	if err != nil {
		t.Fatalf("UploadInventory: %v", err)
	}

	return seeded{client: fc, manifest: info.Path}
}

func TestPipelineRun_ReplacesPreviousIndex(t *testing.T) {
	s := seedManifest(t)
	fc, manifest := s.client, s.manifest
	outDir := filepath.Join(t.TempDir(), "index")

	for range 2 {
		if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, outDir); err != nil {
			t.Fatalf("Run: %v", err)
		}
	}
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatalf("open rebuilt index: %v", err)
	}
	defer idx.Close()
	if idx.Count() == 0 {
		t.Error("rebuilt index is empty")
	}
	entries, err := os.ReadDir(filepath.Dir(outDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("staging or replaced directories left behind: %v", entries)
	}
}

func TestPipelineRun_FailedFinalSpillFailsTheBuild(t *testing.T) {
	s := seedManifest(t)
	fc, manifest := s.client, s.manifest
	outDir := filepath.Join(t.TempDir(), "index")
	// A small inventory spills only once, at the end of each worker. A
	// temp "dir" that is a regular file makes that spill fail for any
	// user, root included.
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := extsort.DefaultConfig()
	cfg.TempDir = notADir

	if _, err := extsort.NewPipeline(cfg, fc).Run(t.Context(), manifest, outDir); err == nil {
		t.Fatal("Run succeeded although every run file failed to write")
	}
	if _, err := os.Stat(outDir); !os.IsNotExist(err) {
		t.Errorf("failed build left something at outDir: %v", err)
	}
}

func TestPipelineRun_FailureKeepsPreviousIndex(t *testing.T) {
	s := seedManifest(t)
	fc, manifest := s.client, s.manifest
	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, outDir); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	before, err := os.ReadFile(filepath.Join(outDir, format.ManifestFile))
	if err != nil {
		t.Fatal(err)
	}

	missing := manifest[:len(manifest)-len("manifest.json")] + "missing.json"
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), missing, outDir); err == nil {
		t.Fatal("Run succeeded with a missing manifest")
	}

	after, err := os.ReadFile(filepath.Join(outDir, format.ManifestFile))
	if err != nil {
		t.Fatalf("previous index damaged by a failed build: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Error("previous index changed by a failed build")
	}
	entries, err := os.ReadDir(filepath.Dir(outDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("staging directory left behind: %v", entries)
	}
}

// uploadCSVInventory writes a one-file gzipped CSV inventory with the
// given schema and rows and returns its manifest URI.
func uploadCSVInventory(t *testing.T, fc *s3fetch.Client, schema string, rows [][]string) string {
	t.Helper()

	return uploadCSVChunk(t, fc, "inv/data/part-0.csv.gz", true, schema, rows)
}

// uploadCSVChunk writes a one-file CSV inventory whose file is chunkKey,
// gzipped or not, and returns its manifest URI.
func uploadCSVChunk(t *testing.T, fc *s3fetch.Client, chunkKey string, gzipped bool, schema string, rows [][]string) string {
	t.Helper()
	bucket := s3test.Bucket(t, fc.Raw())
	var buf bytes.Buffer
	var out io.Writer = &buf
	gz := gzip.NewWriter(&buf)
	if gzipped {
		out = gz
	}
	w := csv.NewWriter(out)
	if err := w.WriteAll(rows); err != nil {
		t.Fatal(err)
	}
	if gzipped {
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
	}
	put := func(key string, body []byte) {
		if _, err := fc.Raw().PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		}); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	put(chunkKey, buf.Bytes())
	manifest, err := json.Marshal(map[string]any{
		"sourceBucket":      "src",
		"destinationBucket": "arn:aws:s3:::" + bucket,
		"version":           "2016-11-30",
		"fileFormat":        "CSV",
		"fileSchema":        schema,
		"files":             []map[string]any{{"key": chunkKey, "size": buf.Len()}},
	})
	if err != nil {
		t.Fatal(err)
	}
	put("inv/manifest.json", manifest)

	return "s3://" + bucket + "/inv/manifest.json"
}

// An all-versions CSV inventory: keys are URL-encoded, delete markers
// have no size and are not objects, noncurrent versions are stored bytes.
func TestPipelineRun_CSVEncodedKeysAndVersions(t *testing.T) {
	fc := s3test.FetchClient(t)
	manifest := uploadCSVInventory(t, fc,
		"Bucket, Key, VersionId, IsLatest, IsDeleteMarker, Size, StorageClass",
		[][]string{
			{"src", "my+folder/a%2Bb.txt", "v2", "true", "false", "100", "STANDARD"},
			{"src", "my+folder/a%2Bb.txt", "v1", "false", "false", "40", "STANDARD"},
			{"src", "my+folder/gone.txt", "v3", "true", "true", "", ""},
			{"src", "donn%C3%A9es/x.csv", "v1", "true", "false", "7", "GLACIER"},
		})
	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, outDir); err != nil {
		t.Fatalf("Run: %v", err)
	}
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	cases := []struct {
		prefix         string
		objects, bytes uint64
	}{
		{"my folder/", 2, 140}, // two stored versions; the delete marker is not counted
		{"données/", 1, 7},
		{"", 3, 147},
	}
	for _, c := range cases {
		st, ok := idx.StatsForPrefix(c.prefix)
		if !ok {
			t.Errorf("prefix %q missing", c.prefix)

			continue
		}
		if st.ObjectCount != c.objects || st.TotalBytes != c.bytes {
			t.Errorf("prefix %q = %d objects / %d bytes, want %d / %d", c.prefix, st.ObjectCount, st.TotalBytes, c.objects, c.bytes)
		}
	}
	if _, ok := idx.StatsForPrefix("my+folder/"); ok {
		t.Error("undecoded prefix my+folder/ present in the index")
	}
}

// A CSV chunk many times the reader's 64 KiB line buffer, whose rows
// switch storage class / access tier every few rows, gets per-tier stats
// equal to a direct count. Every line has the same length and its storage
// class at the same offset, and GLACIER_IR and ONEZONE_IA are the same
// length, so a tier cache that kept a view of the buffer after a refill
// would read another row's class and match the wrong pair.
func TestPipelineRun_CSVTierStatsAcrossBufferRefills(t *testing.T) {
	const (
		rows     = 60_000
		prefixes = 7
		lineLen  = 96
	)
	type pair struct{ class, access string }
	pairs := []pair{
		{"GLACIER_IR", ""},
		{"ONEZONE_IA", ""},
		{"GLACIER_IR", ""},
		{"ONEZONE_IA", ""},
		{"STANDARD", ""},
		{"GLACIER", ""},
		{"INTELLIGENT_TIERING", "FREQUENT"},
		{"INTELLIGENT_TIERING", "INFREQUENT"},
		{"INTELLIGENT_TIERING", "ARCHIVE_INSTANT_ACCESS"},
	}
	mapping := tiers.NewMapping()
	type tierKey struct {
		prefix string
		tier   tiers.ID
	}
	want := map[tierKey]format.TierBreakdown{}
	rng := rand.New(rand.NewPCG(1, 2))
	records := make([][]string, 0, rows)
	var p pair
	for i := range rows {
		if i == 0 || rng.IntN(3) == 0 {
			p = pairs[rng.IntN(len(pairs))]
		}
		size := rng.Uint64N(2 * tiers.SmallObjectThresholdBytes)
		dir := fmt.Sprintf("d%d/", i%prefixes)
		head := fmt.Sprintf("src,%s,%s,%010d,", p.class, p.access, size)
		key := dir + fmt.Sprintf("k%06d-", i)
		key += strings.Repeat("x", lineLen-1-len(head)-len(key))
		records = append(records, []string{"src", p.class, p.access, fmt.Sprintf("%010d", size), key})

		tier := tiers.Resolve(mapping.FromS3(p.class, p.access), size)
		for _, prefix := range []string{"", dir} {
			tb := want[tierKey{prefix, tier}]
			tb.ObjectCount++
			tb.Bytes += size
			want[tierKey{prefix, tier}] = tb
		}
	}
	fc := s3test.FetchClient(t)
	manifest := uploadCSVInventory(t, fc, "Bucket, StorageClass, IntelligentTieringAccessTier, Size, Key", records)
	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, outDir); err != nil {
		t.Fatalf("Run: %v", err)
	}
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	got := map[tierKey]format.TierBreakdown{}
	for i := range prefixes {
		for _, prefix := range []string{"", fmt.Sprintf("d%d/", i)} {
			pos, ok := idx.Lookup(prefix)
			if !ok {
				t.Fatalf("prefix %q missing", prefix)
			}
			for _, tb := range idx.TierBreakdown(pos) {
				got[tierKey{prefix, tb.TierID}] = format.TierBreakdown{ObjectCount: tb.ObjectCount, Bytes: tb.Bytes}
			}
		}
	}
	for k, w := range want {
		if g := got[k]; g != w {
			t.Errorf("prefix %q tier %d = %d objects / %d bytes, want %d / %d", k.prefix, k.tier, g.ObjectCount, g.Bytes, w.ObjectCount, w.Bytes)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("prefix %q has unexpected tier %d", k.prefix, k.tier)
		}
	}
}

// EXPRESS_ONEZONE, OUTPOSTS and SNOW rows are counted under their own
// tiers, not STANDARD.
func TestPipelineRun_CSVNewStorageClassTiers(t *testing.T) {
	fc := s3test.FetchClient(t)
	manifest := uploadCSVInventory(t, fc, "Bucket, Key, Size, StorageClass",
		[][]string{
			{"src", "a/std.txt", "1", "STANDARD"},
			{"src", "a/express.txt", "10", "EXPRESS_ONEZONE"},
			{"src", "a/outposts.txt", "100", "OUTPOSTS"},
			{"src", "a/snow.txt", "1000", "SNOW"},
			{"src", "a/snow2.txt", "2000", "snow"},
		})
	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, outDir); err != nil {
		t.Fatalf("Run: %v", err)
	}
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	pos, ok := idx.Lookup("a/")
	if !ok {
		t.Fatal("prefix a/ missing")
	}
	want := []format.TierBreakdown{
		{TierID: tiers.Standard, TierName: "STANDARD", ObjectCount: 1, Bytes: 1},
		{TierID: tiers.ExpressOneZone, TierName: "EXPRESS_ONEZONE", ObjectCount: 1, Bytes: 10},
		{TierID: tiers.Outposts, TierName: "OUTPOSTS", ObjectCount: 1, Bytes: 100},
		{TierID: tiers.Snow, TierName: "SNOW", ObjectCount: 2, Bytes: 3000},
	}
	if got := idx.TierBreakdown(pos); !slices.Equal(got, want) {
		t.Errorf("a/ tiers = %+v, want %+v", got, want)
	}
}

// A CSV chunk is gunzipped when its content is gzip, whatever its key
// says: reading gzip as text would fail every row as an anomaly, or with
// AllowAnomalies index garbage.
func TestPipelineRun_CSVGzipDetectedByContent(t *testing.T) {
	for _, c := range []struct {
		key     string
		gzipped bool
	}{
		{"inv/data/part-0.csv", true},
		{"inv/data/part-0.csv.gz", false},
		{"inv/data/part-0.csv", false},
	} {
		t.Run(fmt.Sprintf("%s/gzipped=%v", c.key, c.gzipped), func(t *testing.T) {
			fc := s3test.FetchClient(t)
			manifest := uploadCSVChunk(t, fc, c.key, c.gzipped, "Bucket, Key, Size",
				[][]string{{"src", "a/x.txt", "100"}, {"src", "a/b/y.txt", "23"}})
			outDir := filepath.Join(t.TempDir(), "index")
			if _, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, outDir); err != nil {
				t.Fatalf("Run: %v", err)
			}
			idx, err := indexread.Open(outDir)
			if err != nil {
				t.Fatal(err)
			}
			defer idx.Close()
			if st, ok := idx.StatsForPrefix("a/"); !ok || st.ObjectCount != 2 || st.TotalBytes != 123 {
				t.Errorf("a/ = %+v, %v; want 2 objects, 123 bytes", st, ok)
			}
		})
	}
}

// A row that can't be read exactly fails the build (exact answers):
// here a size that doesn't parse. AllowAnomalies accepts it, indexed as 0.
func TestPipelineRun_FailsOnAnomalousRows(t *testing.T) {
	fc := s3test.FetchClient(t)
	manifest := uploadCSVInventory(t, fc, "Bucket, Key, Size",
		[][]string{
			{"src", "a/ok.txt", "100"},
			{"src", "a/bad.txt", "12x"},
		})

	_, err := extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, filepath.Join(t.TempDir(), "index"))
	if !errors.Is(err, extsort.ErrInventoryAnomalies) {
		t.Fatalf("Run error = %v, want ErrInventoryAnomalies", err)
	}

	cfg := extsort.DefaultConfig()
	cfg.AllowAnomalies = true
	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(cfg, fc).Run(t.Context(), manifest, outDir); err != nil {
		t.Fatalf("Run with AllowAnomalies: %v", err)
	}
	idx, err := indexread.Open(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if st, ok := idx.StatsForPrefix("a/"); !ok || st.ObjectCount != 2 || st.TotalBytes != 100 {
		t.Errorf("a/ = %+v, %v; want 2 objects, 100 bytes", st, ok)
	}
}

// A downloaded chunk whose size differs from its manifest entry (a
// truncated or replaced file) fails the build.
func TestPipelineRun_FailsOnChunkSizeMismatch(t *testing.T) {
	fc := s3test.FetchClient(t)
	manifest := uploadCSVInventory(t, fc, "Bucket, Key, Size", [][]string{{"src", "a/ok.txt", "100"}})
	bucket, key := splitS3URI(t, manifest)
	out, err := fc.Raw().GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.NewDecoder(out.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	_ = out.Body.Close()
	file := m["files"].([]any)[0].(map[string]any) //nolint:forcetypeassert // our own fixture
	file["size"] = file["size"].(float64) + 1      //nolint:forcetypeassert // our own fixture
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fc.Raw().PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
	}); err != nil {
		t.Fatal(err)
	}

	_, err = extsort.NewPipeline(extsort.DefaultConfig(), fc).Run(t.Context(), manifest, filepath.Join(t.TempDir(), "index"))
	if !errors.Is(err, extsort.ErrChunkSizeMismatch) {
		t.Fatalf("Run error = %v, want ErrChunkSizeMismatch", err)
	}
}

// splitS3URI splits "s3://bucket/key" into bucket and key.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func splitS3URI(t *testing.T, uri string) (string, string) {
	t.Helper()
	rest, ok := strings.CutPrefix(uri, "s3://")
	bucket, key, found := strings.Cut(rest, "/")
	if !ok || !found {
		t.Fatalf("not an s3 URI: %q", uri)
	}

	return bucket, key
}

// A build keeps its scratch (downloads, run files, MPHF scratch) beside
// the output, never in os.TempDir: TMPDIR here is read-only, and the
// output's parent holds only the index afterwards.
func TestPipelineRun_ScratchStaysBesideTheIndex(t *testing.T) {
	s := seedManifest(t)
	readOnly := t.TempDir()
	if err := os.Chmod(readOnly, 0o500); err != nil { //nolint:gosec // the test needs a directory nothing can write to
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(readOnly, 0o700) }) //nolint:gosec // restore so t.TempDir can clean up
	t.Setenv("TMPDIR", readOnly)

	outDir := filepath.Join(t.TempDir(), "index")
	if _, err := extsort.NewPipeline(extsort.DefaultConfig(), s.client).Run(t.Context(), s.manifest, outDir); err != nil {
		t.Fatalf("Run with a read-only TMPDIR: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(outDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "index" {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("output parent holds %v, want only the index", names)
	}
}

// TestPipelineRun_ResidualsInMemoryMatchSpilled: workers' last states
// merged from memory build exactly the index they build when heap
// pressure sends them to run files instead.
func TestPipelineRun_ResidualsInMemoryMatchSpilled(t *testing.T) {
	s := seedManifest(t)
	build := func(memoryLimit int64) (*extsort.Result, string) {
		t.Helper()
		outDir := filepath.Join(t.TempDir(), "index")
		if memoryLimit > 0 {
			prev := debug.SetMemoryLimit(memoryLimit)
			defer debug.SetMemoryLimit(prev)
		}
		res, err := extsort.NewPipeline(extsort.DefaultConfig(), s.client).Run(t.Context(), s.manifest, outDir)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}

		return res, outDir
	}
	inMemory, memDir := build(0)
	if inMemory.MemoryRuns == 0 || inMemory.RunFilesCreated != 0 {
		t.Fatalf("without pressure: %d memory runs, %d run files; want the last states in memory", inMemory.MemoryRuns, inMemory.RunFilesCreated)
	}
	spilled, fileDir := build(1) // every heap reading is pressure
	if spilled.MemoryRuns != 0 || spilled.RunFilesCreated == 0 {
		t.Fatalf("under pressure: %d memory runs, %d run files; want run files", spilled.MemoryRuns, spilled.RunFilesCreated)
	}

	a, err := indexread.Open(memDir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := indexread.Open(fileDir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if a.Count() != b.Count() || a.Count() == 0 {
		t.Fatalf("prefix counts %d and %d", a.Count(), b.Count())
	}
	for pos := range a.Count() {
		pa, errA := a.PrefixString(pos)
		pb, errB := b.PrefixString(pos)
		if errA != nil || errB != nil || pa != pb || a.Stats(pos) != b.Stats(pos) || !indexread.TiersEqual(a.TierBreakdown(pos), b.TierBreakdown(pos)) {
			t.Fatalf("position %d: %q %+v vs %q %+v (%v, %v)", pos, pa, a.Stats(pos), pb, b.Stats(pos), errA, errB)
		}
	}
}

// A build cancelled while chunks are downloading ahead of the workers
// leaves no downloaded chunk in scratch: a fetcher holding one when the
// build stops removes it.
func TestPipelineRun_CancelledIngestLeavesNoChunks(t *testing.T) {
	raw := s3test.RawClient(t)
	bucket := s3test.Bucket(t, raw)
	info, err := seeder.UploadMultiChunkInventory(t.Context(), raw, seeder.Config{
		Target:  seeder.TargetS3,
		Objects: 20_000,
		Preset:  "small",
		Seed:    11,
		Logger:  zerolog.Nop(),
	}, seeder.S3Config{Bucket: bucket, Prefix: "inv/", SrcBucket: "src"}, 1, 11, time.Now().UTC().Truncate(time.Minute), 4*runtime.NumCPU())
	if err != nil {
		t.Fatalf("UploadMultiChunkInventory: %v", err)
	}

	scratch := t.TempDir()
	cfg := extsort.DefaultConfig()
	cfg.TempDir = scratch
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		// Cancel once downloads are under way.
		for ctx.Err() == nil {
			if matches, _ := filepath.Glob(filepath.Join(scratch, "s3download-*.tmp")); len(matches) > 0 {
				time.Sleep(20 * time.Millisecond)
				cancel()

				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	fc := s3test.SlowFetchClient(t, 5*time.Millisecond, 512<<10)
	if _, err := extsort.NewPipeline(cfg, fc).Run(ctx, info.Path, filepath.Join(t.TempDir(), "index")); err == nil {
		t.Fatal("Run succeeded although it was cancelled mid-ingest")
	}
	if left, _ := filepath.Glob(filepath.Join(scratch, "s3download-*.tmp")); len(left) != 0 {
		t.Errorf("%d downloaded chunks left in scratch after the cancelled build", len(left))
	}
}

// TestPipelineRun_CancelWaitsForDownloads cancels a build while a chunk
// download is stalled and checks Run returns only after the downloads
// have unwound and removed their files, never while one may still
// write to scratch.
func TestPipelineRun_CancelWaitsForDownloads(t *testing.T) {
	raw := s3test.RawClient(t)
	bucket := s3test.Bucket(t, raw)
	info, err := seeder.UploadMultiChunkInventory(t.Context(), raw, seeder.Config{
		Target:  seeder.TargetS3,
		Objects: 2_000,
		Preset:  "small",
		Seed:    12,
		Logger:  zerolog.Nop(),
	}, seeder.S3Config{Bucket: bucket, Prefix: "inv/", SrcBucket: "src"}, 1, 12, time.Now().UTC().Truncate(time.Minute), 4)
	if err != nil {
		t.Fatalf("UploadMultiChunkInventory: %v", err)
	}

	scratch := t.TempDir()
	cfg := extsort.DefaultConfig()
	cfg.TempDir = scratch
	fc, gate := s3test.GatedFetchClient(t, "/data/")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, runErr := extsort.NewPipeline(cfg, fc).Run(ctx, info.Path, filepath.Join(t.TempDir(), "index"))
		done <- runErr
	}()

	select {
	case <-gate.Stalled():
	case runErr := <-done:
		t.Fatalf("Run returned before any chunk download started: %v", runErr)
	}
	cancel()

	const grace = 300 * time.Millisecond
	select {
	case <-done:
		t.Fatal("Run returned while a chunk download was still in flight")
	case <-time.After(grace):
	}
	gate.Open()
	if runErr := <-done; runErr == nil {
		t.Fatal("Run succeeded although it was cancelled mid-ingest")
	}
	if left, _ := filepath.Glob(filepath.Join(scratch, "s3download-*.tmp")); len(left) != 0 {
		t.Errorf("%d downloaded chunks left in scratch after the cancelled build", len(left))
	}
}
