package s3disco_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/miniotest"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/eunmann/s3-inv-db/internal/seeder"
	"github.com/rs/zerolog"
)

func TestDiscoverer_List_AgainstMinIO(t *testing.T) {
	client := miniotest.RawClient(t)
	ctx := t.Context()
	bucket := miniotest.Bucket(t, client)

	// Upload two inventories with two timestamp folders each, via the
	// seeder package so we exercise the same code path the CLI uses.
	now := time.Now().UTC()
	earlier := now.Add(-time.Hour)
	srcBucket := "synthetic-prod"
	prefix := "inventory-data/"

	uploadInventoryAt(ctx, t, client, bucket, srcBucket, prefix, "inv-001", earlier, 100, 1001)
	uploadInventoryAt(ctx, t, client, bucket, srcBucket, prefix, "inv-001", now, 100, 1002)
	uploadInventoryAt(ctx, t, client, bucket, srcBucket, prefix, "inv-002", now, 100, 2001)

	d := s3disco.New(client, bucket, prefix)
	got, err := d.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// Discovery now returns one entry PER RUN: inv-001 has two runs,
	// inv-002 has one — total three.
	if len(got) != 3 {
		t.Fatalf("len(List) = %d, want 3 (2 runs of inv-001 + 1 of inv-002); got=%+v", len(got), got)
	}

	nowStamp := now.Format("2006-01-02T15-04Z")
	earlierStamp := earlier.Format("2006-01-02T15-04Z")

	// Group by (src, inv).
	runsByInv := map[string][]string{}
	for _, e := range got {
		if e.SourceBucket != srcBucket {
			t.Errorf("entry has SourceBucket = %q, want %q", e.SourceBucket, srcBucket)
		}
		runsByInv[e.Name] = append(runsByInv[e.Name], e.Run)
	}

	if got := runsByInv["inv-001"]; len(got) != 2 {
		t.Errorf("inv-001 runs = %v, want 2 entries", got)
	} else if got[0] != nowStamp || got[1] != earlierStamp {
		t.Errorf("inv-001 runs out of order: %v, want [%s, %s] (newest first)", got, nowStamp, earlierStamp)
	}
	if got := runsByInv["inv-002"]; len(got) != 1 || got[0] != nowStamp {
		t.Errorf("inv-002 runs = %v, want [%s]", got, nowStamp)
	}
}

func TestDiscoverer_List_PopulatesManifestStats(t *testing.T) {
	client := miniotest.RawClient(t)
	ctx := t.Context()
	bucket := miniotest.Bucket(t, client)

	now := time.Now().UTC()
	uploadInventoryAt(ctx, t, client, bucket, "synthetic-prod", "inventory-data/", "inv-001", now, 100, 9999)

	d := s3disco.New(client, bucket, "inventory-data/")
	got, err := d.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(List) = %d, want 1", len(got))
	}
	entry := got[0]
	if entry.FileCount == 0 {
		t.Errorf("FileCount = 0, want > 0 (manifest should reference at least one data file)")
	}
	if entry.TotalBytes == 0 {
		t.Errorf("TotalBytes = 0, want > 0 (manifest data files should report compressed size)")
	}
	if entry.CreationTimestamp == "" {
		t.Errorf("CreationTimestamp = empty, want manifest creation timestamp")
	}
}

func TestDiscoverer_Find(t *testing.T) {
	client := miniotest.RawClient(t)
	ctx := t.Context()
	bucket := miniotest.Bucket(t, client)

	now := time.Now().UTC()
	// The seeder always names inventories inv-NNN where NNN = index.
	uploadInventoryAt(ctx, t, client, bucket, "src-a", "inv/", "inv-001", now, 50, 7)
	d := s3disco.New(client, bucket, "inv/")

	// Empty run = "give me the latest".
	got, err := d.Find(ctx, "src-a", "inv-001", "")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	want := now.Format("2006-01-02T15-04Z")
	if got.Run != want {
		t.Errorf("Run = %q, want %q", got.Run, want)
	}

	// Exact-run lookup also works.
	got, err = d.Find(ctx, "src-a", "inv-001", want)
	if err != nil {
		t.Fatalf("Find exact run: %v", err)
	}
	if got.Run != want {
		t.Errorf("exact Run = %q, want %q", got.Run, want)
	}

	if _, err := d.Find(ctx, "", "inv-001", ""); err == nil {
		t.Error("Find with empty src should error")
	}
	if _, err := d.Find(ctx, "src-a", "inv-001", "2099-01-01T00-00Z"); err == nil {
		t.Error("Find with unknown run should error")
	}
}

func TestDiscoverer_EmptyBucket(t *testing.T) {
	client := miniotest.RawClient(t)
	ctx := t.Context()
	bucket := miniotest.Bucket(t, client)

	d := s3disco.New(client, bucket, "inventory-data/")
	got, err := d.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(List) = %d on empty bucket, want 0", len(got))
	}
}

// uploadInventoryAt drives the seeder for a specific (src, invID, stamp).
// The seeder package's run-stamp is "now" by default; we use the lower-level
// helper directly so each upload uses a controlled timestamp.
func uploadInventoryAt(ctx context.Context, t *testing.T, client *s3.Client, bucket, srcBucket, prefix, invID string, stamp time.Time, objects int, seed int64) {
	t.Helper()
	// seeder.uploadInventoryToS3 derives the inv-id from index, so we pick
	// an index that produces our desired id. inv-001 → index=1, inv-002 →
	// index=2, etc.
	index := indexFromID(t, invID)
	_, err := seeder.UploadInventory(ctx, client, seeder.Config{
		Target:  seeder.TargetS3,
		Objects: objects,
		Preset:  "small",
		Seed:    seed,
		Logger:  zerolog.Nop(),
	}, seeder.S3Config{
		Bucket:    bucket,
		Prefix:    prefix,
		SrcBucket: srcBucket,
	}, index, seed, stamp)
	if err != nil {
		t.Fatalf("upload inventory %s: %v", invID, err)
	}
}

func indexFromID(t *testing.T, id string) int {
	t.Helper()
	switch id {
	case "inv-001":
		return 1
	case "inv-002":
		return 2
	}
	t.Fatalf("unsupported test inv-id %q", id)

	return 0
}

func TestRunFolderRE(t *testing.T) {
	cases := []struct {
		name  string
		match bool
	}{
		{"2026-05-13T03-02Z", true},       // minute-granularity (AWS)
		{"2026-05-13T03-02-15Z", true},    // second-granularity (seeder)
		{"data", false},                   // never a run folder
		{"9-trash", false},                // loose digit prefix must NOT match
		{"2026-05-13T03-02", false},       // missing trailing Z
		{"2026-05-13", false},             // date only
		{"", false},                       // empty
		{"2026-05-13T03-02Zextra", false}, // trailing junk
		{"2026-13-99T99-99Z", true},       // shape matches even with absurd values
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := s3disco.RunFolderRE.MatchString(c.name); got != c.match {
				t.Errorf("MatchString(%q) = %v, want %v", c.name, got, c.match)
			}
		})
	}
}
