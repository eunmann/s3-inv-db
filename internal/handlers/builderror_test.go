package handlers_test

import (
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
)

// TestSummarizeBuildError: a failed build's error chain, as the job
// store and catalog record it, reads as one sentence naming the cause,
// never the wrappers or AWS request IDs.
func TestSummarizeBuildError(t *testing.T) {
	const missingKey = "s3://s3-inv/inventory-data/analytics-logs/inv-001/data/006ade6b8b-0001-chunk-0001.csv.gz"
	tests := []struct {
		name, raw, want string
	}{
		{
			name: "missing data file",
			raw: "gated load analytics-logs/inv-001/2026-10-25T20-50Z: load analytics-logs/inv-001/2026-10-25T20-50Z: build index: run pipeline: ingest phase: chunk worker: chunk 1: download object: download " +
				missingKey + ": operation error S3: GetObject, https response error StatusCode: 404, RequestID: RD3RNTQ2KIO4OQZ6, HostID: Cx6k…=, NoSuchKey: The specified key does not exist.",
			want: "An inventory data file is missing from S3: " + missingKey + ". The inventory may have been deleted or moved.",
		},
		{
			name: "access denied",
			raw: "gated load b/i/2026-10-25T20-50Z: load b/i/2026-10-25T20-50Z: build index: fetch manifest: get s3://inv-bucket/b/i/2026-10-25T20-50Z/manifest.json: " +
				"operation error S3: GetObject, https response error StatusCode: 403, RequestID: 8Q2V, HostID: zx9=, api error AccessDenied: Access Denied",
			want: "Access denied reading s3://inv-bucket/b/i/2026-10-25T20-50Z/manifest.json. Check the server's S3 permissions.",
		},
		{
			name: "access denied without a key",
			raw:  "list runs: operation error S3: ListObjectsV2, https response error StatusCode: 403, RequestID: X, HostID: Y, api error AccessDenied: Access Denied",
			want: "Access denied reading from S3. Check the server's S3 permissions.",
		},
		{
			name: "cancelled",
			raw:  "gated load b/i/r: load b/i/r: build index: run pipeline: ingest phase: context canceled",
			want: "Cancelled.",
		},
		{
			name: "restart marked by the server at boot",
			raw:  "interrupted by server restart",
			want: "Interrupted by a server restart. Load it again to rebuild it.",
		},
		{
			name: "shutdown while the job ran",
			raw:  "interrupted: the server shut down",
			want: "Interrupted by a server restart. Load it again to rebuild it.",
		},
		{
			name: "disk budget refused",
			raw:  "gated load b/i/r: load refused by disk-budget planner: need 1048576 bytes but only 0 available even after evicting every auto-loaded run; raise --max-index-disk",
			want: "Not enough room in the index disk budget. Unload or unpin a run, or raise --max-index-disk.",
		},
		{
			name: "disk full",
			raw:  "build index: run pipeline: ingest phase: spill: write /var/cache/s3-inv-db/tmp/run-0007.bin: no space left on device",
			want: "The server ran out of disk space while building the index.",
		},
		{
			name: "checksum",
			raw:  "load b/i/r: open index: verify index: file mphf.bin: checksum mismatch",
			want: "A file failed its checksum check and may be corrupt. Load it again to rebuild it.",
		},
		{
			name: "manifest",
			raw:  "gated load b/i/r: load b/i/r: build index: fetch manifest: manifest has no files",
			want: "The inventory's manifest could not be used: manifest has no files.",
		},
		{
			name: "unknown storage class",
			raw:  `gated load b/i/r: load b/i/r: build index: run pipeline: ingest phase: chunk worker: chunk 3: classify "a/odd.txt" in b/i/data/part-0.csv.gz: unknown storage class "GLACIER_DEEP"`,
			want: `Unknown storage class "GLACIER_DEEP".`,
		},
		{
			name: "unknown storage class on a key naming a checksum",
			raw:  `build index: run pipeline: ingest phase: chunk worker: chunk 3: classify "checksums/a.sha256" in b/i/data/part-0.csv.gz: unknown storage class "GLACIER_DEEP"`,
			want: `Unknown storage class "GLACIER_DEEP".`,
		},
		{
			name: "unknown storage class on a key naming a manifest",
			raw:  `build index: run pipeline: ingest phase: chunk worker: chunk 3: classify "app/manifest.json" in b/i/data/part-0.csv.gz: unknown storage class "GLACIER_DEEP"`,
			want: `Unknown storage class "GLACIER_DEEP".`,
		},
		{
			name: "unknown storage class on a key with a colon and other keywords",
			raw:  `build index: run pipeline: ingest phase: chunk worker: chunk 3: classify "AccessDenied: context canceled/no space left" in b/i/data/part-0.csv.gz: unknown storage class "GLACIER_DEEP"`,
			want: `Unknown storage class "GLACIER_DEEP".`,
		},
		{
			name: "unknown access tier on a key naming a manifest",
			raw:  `build index: run pipeline: ingest phase: chunk worker: chunk 3: classify "manifest.json" in b/i/data/part-0.csv.gz: unknown Intelligent-Tiering access tier "DEEP"`,
			want: `Unknown Intelligent-Tiering access tier "DEEP".`,
		},
		{
			name: "unknown storage class whose value holds a colon",
			raw:  `build index: run pipeline: ingest phase: chunk worker: chunk 3: classify "a" in b/i/data/part-0.csv.gz: unknown storage class "X: Y"`,
			want: `Unknown storage class "X: Y".`,
		},
		{
			name: "other: a key naming an unknown storage class",
			raw:  `build index: run pipeline: ingest phase: chunk worker: chunk 3: parse row "unknown storage class \"X\"": unexpected EOF`,
			want: "Unexpected EOF.",
		},
		{
			name: "other: the innermost message",
			raw:  "gated load b/i/r: load b/i/r: build index: run pipeline: ingest phase: chunk worker: chunk 3: parse row 17: unexpected EOF",
			want: "Unexpected EOF.",
		},
		{
			name: "other: a bare message",
			raw:  "network broken",
			want: "Network broken.",
		},
		{name: "none", raw: "  ", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := handlers.SummarizeBuildErrorForTest(tt.raw)
			if got != tt.want {
				t.Errorf("summary\n got %q\nwant %q", got, tt.want)
			}
			if strings.Contains(got, "RequestID") || strings.Contains(got, "gated load") {
				t.Errorf("summary keeps the raw chain: %q", got)
			}
		})
	}
}

func TestSummarizeBuildError_CapsALongCause(t *testing.T) {
	got := handlers.SummarizeBuildErrorForTest("parse: " + strings.Repeat("x", 500))
	if n := len([]rune(got)); n > 202 || !strings.HasSuffix(got, "…") {
		t.Errorf("summary of a 500-rune cause is %d runes: %q", n, got)
	}
}

// TestSummarizePollError: a discovery poll lists and reads manifests,
// it builds nothing, so its summary never gives a build's advice.
func TestSummarizePollError(t *testing.T) {
	tests := []struct {
		name, raw, want string
	}{
		{
			name: "missing bucket",
			raw:  "list runs s3://gone-bucket/inv/: operation error S3: ListObjectsV2, https response error StatusCode: 404, RequestID: X, HostID: Y, api error NoSuchBucket: The specified bucket does not exist",
			want: "Nothing found in S3 at s3://gone-bucket/inv/. Check that the inventory's bucket and prefix still exist.",
		},
		{
			name: "missing without a URI",
			raw:  "list runs: operation error S3: ListObjectsV2, https response error StatusCode: 404, api error NoSuchBucket: The specified bucket does not exist",
			want: "The inventory's bucket or prefix was not found in S3. Check that it still exists.",
		},
		{
			name: "access denied",
			raw:  "list runs: operation error S3: ListObjectsV2, https response error StatusCode: 403, RequestID: X, HostID: Y, api error AccessDenied: Access Denied",
			want: "Access denied reading from S3. Check the server's S3 permissions.",
		},
		{
			name: "manifest checksum",
			raw:  "poll src/inv: read manifest s3://dst/src/inv/2026-10-25T20-50Z/manifest.json: manifest checksum mismatch",
			want: "The inventory's manifest could not be used: manifest checksum mismatch.",
		},
		{
			name: "no space on a poll is just the cause",
			raw:  "poll src/inv: write state: no space left on device",
			want: "No space left on device.",
		},
		{name: "none", raw: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := handlers.SummarizePollErrorForTest(tt.raw); got != tt.want {
				t.Errorf("summary\n got %q\nwant %q", got, tt.want)
			}
		})
	}
}
