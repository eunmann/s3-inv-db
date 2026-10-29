package extsort

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
	"github.com/rs/zerolog"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func nopLogger() *zerolog.Logger {
	l := zerolog.Nop()

	return &l
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	return string(b)
}

// siblings lists the entries next to dir's own name, so tests can assert
// no staging or replaced-index directories were left behind.
func siblings(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(dir))
	if err != nil {
		t.Fatalf("read parent: %v", err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}

	return out
}

func TestCheckOutDir(t *testing.T) {
	root := t.TempDir()
	index := filepath.Join(root, "index")
	writeFile(t, filepath.Join(index, format.ManifestFile), "{}")
	empty := filepath.Join(root, "empty")
	if err := os.Mkdir(empty, 0o750); err != nil {
		t.Fatal(err)
	}
	user := filepath.Join(root, "user")
	writeFile(t, filepath.Join(user, "notes.txt"), "keep me")
	file := filepath.Join(root, "file")
	writeFile(t, file, "x")

	cases := []struct {
		name    string
		dir     string
		wantErr bool
	}{
		{"missing", filepath.Join(root, "missing"), false},
		{"empty", empty, false},
		{"previous_index", index, false},
		{"user_files", user, true},
		{"regular_file", file, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkOutDir(c.dir)
			if c.wantErr != errors.Is(err, ErrOutDirNotIndex) {
				t.Errorf("checkOutDir(%s) = %v, wantErr %v", c.name, err, c.wantErr)
			}
		})
	}
}

func TestPublishIndex_IntoMissingDir(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "out")
	stage, err := newStagingDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(stage, format.ManifestFile), "new")

	if err := publishIndex(nopLogger(), stage, outDir); err != nil {
		t.Fatalf("publishIndex: %v", err)
	}
	if got := readFile(t, filepath.Join(outDir, format.ManifestFile)); got != "new" {
		t.Errorf("manifest = %q, want new", got)
	}
	if got := siblings(t, outDir); len(got) != 1 {
		t.Errorf("leftover siblings: %v", got)
	}
}

func TestPublishIndex_ReplacesPreviousIndex(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "out")
	writeFile(t, filepath.Join(outDir, format.ManifestFile), "old")
	writeFile(t, filepath.Join(outDir, "core_stats.bin"), "old")
	stage, err := newStagingDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(stage, format.ManifestFile), "new")

	if err := publishIndex(nopLogger(), stage, outDir); err != nil {
		t.Fatalf("publishIndex: %v", err)
	}
	if got := readFile(t, filepath.Join(outDir, format.ManifestFile)); got != "new" {
		t.Errorf("manifest = %q, want new", got)
	}
	if _, err := os.Stat(filepath.Join(outDir, "core_stats.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("previous index file survived the replace: %v", err)
	}
	if got := siblings(t, outDir); len(got) != 1 {
		t.Errorf("leftover siblings: %v", got)
	}
}

func TestPublishIndex_RefusesNonIndexDir(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "out")
	writeFile(t, filepath.Join(outDir, "notes.txt"), "keep me")
	stage, err := newStagingDir(outDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := publishIndex(nopLogger(), stage, outDir); !errors.Is(err, ErrOutDirNotIndex) {
		t.Fatalf("publishIndex err = %v, want ErrOutDirNotIndex", err)
	}
	if got := readFile(t, filepath.Join(outDir, "notes.txt")); got != "keep me" {
		t.Errorf("user file changed: %q", got)
	}
}

func TestRun_RefusesNonIndexOutDirBeforeWork(t *testing.T) {
	outDir := t.TempDir()
	writeFile(t, filepath.Join(outDir, "notes.txt"), "keep me")

	// A nil S3 client proves Run fails before touching the network.
	p := NewPipeline(DefaultConfig(), nil)
	if _, err := p.Run(t.Context(), "s3://bucket/manifest.json", outDir); !errors.Is(err, ErrOutDirNotIndex) {
		t.Fatalf("Run err = %v, want ErrOutDirNotIndex", err)
	}
	if got := readFile(t, filepath.Join(outDir, "notes.txt")); got != "keep me" {
		t.Errorf("user file changed: %q", got)
	}
}

func TestIndexBuilderCleanup_KeepsPreexistingOutDir(t *testing.T) {
	outDir := t.TempDir()
	writeFile(t, filepath.Join(outDir, "notes.txt"), "keep me")
	b, err := NewIndexBuilder(outDir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	b.cleanup()

	if got := readFile(t, filepath.Join(outDir, "notes.txt")); got != "keep me" {
		t.Errorf("cleanup touched a file it did not create: %q", got)
	}
}

func TestIndexBuilderCleanup_RemovesOutDirItCreated(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "fresh")
	b, err := NewIndexBuilder(outDir, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	b.cleanup()

	if _, err := os.Stat(outDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("outDir created by the builder survived cleanup: %v", err)
	}
}

var errDenied = errors.New("s3: access denied")

func TestFirstCause(t *testing.T) {
	denied := fmt.Errorf("worker 0 chunk 3: %w", errDenied)
	cancelled := fmt.Errorf("worker 1 cancelled: %w", context.Canceled)
	cases := []struct {
		name string
		errs []error
		want error
	}{
		{"none", nil, nil},
		{"cause_after_cancelled", []error{cancelled, denied, cancelled}, denied},
		{"only_cancelled", []error{cancelled, cancelled}, cancelled},
		{"deadline_is_a_cause", []error{cancelled, context.DeadlineExceeded}, context.DeadlineExceeded},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ch := make(chan error, len(c.errs))
			for _, e := range c.errs {
				ch <- e
			}
			close(ch)
			if got := firstCause(ch); !errors.Is(got, c.want) || (c.want == nil) != (got == nil) {
				t.Errorf("firstCause = %v, want %v", got, c.want)
			}
		})
	}
}

func TestPublishIndex_TrailingSlashOutDir(t *testing.T) {
	root := t.TempDir()
	outDir := filepath.Join(root, "out")
	writeFile(t, filepath.Join(outDir, format.ManifestFile), "old")
	stage, err := newStagingDir(outDir + "/")
	if err != nil {
		t.Fatal(err)
	}
	if got := filepath.Dir(stage); got != root {
		t.Fatalf("staging dir %s is not a sibling of %s", stage, outDir)
	}
	writeFile(t, filepath.Join(stage, format.ManifestFile), "new")

	if err := publishIndex(nopLogger(), stage, outDir+"/"); err != nil {
		t.Fatalf("publishIndex: %v", err)
	}
	if got := readFile(t, filepath.Join(outDir, format.ManifestFile)); got != "new" {
		t.Errorf("manifest = %q, want new", got)
	}
	if got := siblings(t, outDir); len(got) != 1 {
		t.Errorf("leftover siblings: %v", got)
	}
}

func TestPublishIndex_PreviousIndexRemovalFailureKeepsNewIndex(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	outDir := filepath.Join(t.TempDir(), "out")
	writeFile(t, filepath.Join(outDir, format.ManifestFile), "old")
	locked := filepath.Join(outDir, "locked")
	writeFile(t, filepath.Join(locked, "part.bin"), "old")
	info, err := os.Stat(locked)
	if err != nil {
		t.Fatal(err)
	}
	// Read-only without search permission: its file cannot be unlinked.
	if err := os.Chmod(locked, 0o400); err != nil {
		t.Fatal(err)
	}
	stage, err := newStagingDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, name := range siblings(t, outDir) {
			_ = os.Chmod(filepath.Join(filepath.Dir(outDir), name, "locked"), info.Mode().Perm())
		}
	})
	writeFile(t, filepath.Join(stage, format.ManifestFile), "new")

	if err := publishIndex(nopLogger(), stage, outDir); err != nil {
		t.Fatalf("publishIndex = %v, want nil once the new index is in place", err)
	}
	if got := readFile(t, filepath.Join(outDir, format.ManifestFile)); got != "new" {
		t.Errorf("manifest = %q, want new", got)
	}
}

func TestReclaimStaleDirs_RemovesLeftovers(t *testing.T) {
	root := t.TempDir()
	outDir := filepath.Join(root, "out")
	writeFile(t, filepath.Join(outDir, format.ManifestFile), "current")
	writeFile(t, filepath.Join(root, ".out.building-123", "core_stats.bin"), "partial")
	writeFile(t, filepath.Join(root, ".out.replaced-456", format.ManifestFile), "old")
	writeFile(t, filepath.Join(root, ".out.scratch-789", "run-0001.zst"), "spill")
	writeFile(t, filepath.Join(root, ".other.building-1", "keep"), "x")
	writeFile(t, filepath.Join(root, "out2", "keep"), "x")

	if err := reclaimStaleDirs(nopLogger(), outDir+"/"); err != nil {
		t.Fatalf("reclaimStaleDirs: %v", err)
	}
	got := siblings(t, outDir)
	want := []string{".other.building-1", "out", "out2"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("siblings = %v, want %v", got, want)
	}
	if got := readFile(t, filepath.Join(outDir, format.ManifestFile)); got != "current" {
		t.Errorf("manifest = %q, want current", got)
	}
}

func TestReclaimStaleDirs_RestoresInterruptedReplace(t *testing.T) {
	root := t.TempDir()
	outDir := filepath.Join(root, "out")
	writeFile(t, filepath.Join(root, ".out.replaced-456", format.ManifestFile), "old")

	if err := reclaimStaleDirs(nopLogger(), outDir); err != nil {
		t.Fatalf("reclaimStaleDirs: %v", err)
	}
	if got := readFile(t, filepath.Join(outDir, format.ManifestFile)); got != "old" {
		t.Errorf("manifest = %q, want the restored previous index", got)
	}
	if got := siblings(t, outDir); len(got) != 1 {
		t.Errorf("leftover siblings: %v", got)
	}
}

// buildStagedIndex builds a small index with two tiers, so it has the
// tier_stats subdirectory, into a new staging directory for outDir.
func buildStagedIndex(t *testing.T, outDir string) string {
	t.Helper()
	stage, err := newStagingDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	agg := NewAggregator(64, 0)
	for i := range 40 {
		tier := tiers.Standard
		if i%3 == 0 {
			tier = tiers.GlacierIR
		}
		agg.AddObject(fmt.Sprintf("d%d/s%d/obj%d", i%4, i%7, i), uint64(1000+i), tier)
	}
	b, err := NewIndexBuilder(stage, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SetPresentTiers(agg.PresentTiers()); err != nil {
		t.Fatal(err)
	}
	rows := agg.Drain()
	SortPrefixRows(rows)
	for _, row := range rows {
		if err := b.Add(row); err != nil {
			t.Fatalf("Add(%q): %v", row.Prefix, err)
		}
	}
	if err := b.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	return stage
}

// treeEntries maps every entry under root, relative to it ("." for
// root), to whether it is a directory.
func treeEntries(t *testing.T, root string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("relative path: %w", err)
		}
		out[rel] = d.IsDir()

		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}

	return out
}

// syncRecorder records, relative to stage, every path its sync makes
// durable and whether as a directory, failing the test on a path synced
// twice or after it stopped existing (a stage synced after its rename).
type syncRecorder struct {
	t      *testing.T
	stage  string
	mu     sync.Mutex
	synced map[string]bool
}

func newSyncRecorder(t *testing.T, stage string) *syncRecorder {
	t.Helper()

	return &syncRecorder{t: t, stage: stage, synced: map[string]bool{}}
}

func (r *syncRecorder) sync(path string, dir bool) error {
	rel, err := filepath.Rel(r.stage, path)
	if err != nil {
		return fmt.Errorf("relative path: %w", err)
	}
	if _, err := os.Stat(path); err != nil {
		r.t.Errorf("sync %s after the stage was renamed: %v", rel, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.synced[rel]; dup {
		r.t.Errorf("%s synced twice", rel)
	}
	r.synced[rel] = dir

	return durableSync(path, dir)
}

func (r *syncRecorder) entries() map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.synced
}

// assertSyncedAll checks that every file and directory of the index
// published at outDir, and outDir's parent, was synced as what it is.
func assertSyncedAll(t *testing.T, outDir string, synced map[string]bool) {
	t.Helper()
	published := treeEntries(t, outDir)
	if !published[format.TierStatsDir] {
		t.Fatalf("published index has no %s directory: %v", format.TierStatsDir, published)
	}
	published[".."] = true
	for rel, dir := range published {
		got, ok := synced[rel]
		switch {
		case !ok:
			t.Errorf("%s published without a sync", rel)
		case got != dir:
			t.Errorf("%s synced as dir=%v, is dir=%v", rel, got, dir)
		}
	}
	if len(synced) != len(published) {
		t.Errorf("synced %d entries, published %d", len(synced), len(published))
	}
}

// TestPublishIndex_SyncsEveryFileBeforeRename: every file and directory
// of the published index, tier_stats/ and its files included, is synced
// as a file or a directory while it is still in the stage, and the
// output's parent after the rename.
func TestPublishIndex_SyncsEveryFileBeforeRename(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "out")
	stage := buildStagedIndex(t, outDir)
	rec := newSyncRecorder(t, stage)

	if err := publish(nopLogger(), stage, outDir, rec.sync); err != nil {
		t.Fatalf("publish: %v", err)
	}
	assertSyncedAll(t, outDir, rec.entries())
}

// TestPipelinePublishStage_SyncsUnlessTransient: a build's own index
// is synced in full before it is published; a transient one (uploaded,
// then removed) is published without a single sync.
func TestPipelinePublishStage_SyncsUnlessTransient(t *testing.T) {
	for _, transient := range []bool{false, true} {
		t.Run(fmt.Sprintf("transient=%v", transient), func(t *testing.T) {
			outDir := filepath.Join(t.TempDir(), "out")
			stage := buildStagedIndex(t, outDir)
			rec := newSyncRecorder(t, stage)
			cfg := DefaultConfig()
			cfg.TransientOutput = transient
			p := NewPipeline(cfg, nil)
			p.syncPath = rec.sync

			if err := p.publishStage(nopLogger(), stage, outDir); err != nil {
				t.Fatalf("publishStage: %v", err)
			}
			if _, err := os.Stat(filepath.Join(outDir, format.ManifestFile)); err != nil {
				t.Fatalf("published index: %v", err)
			}
			if transient {
				if got := rec.entries(); len(got) != 0 {
					t.Errorf("transient output synced %v", got)
				}

				return
			}
			assertSyncedAll(t, outDir, rec.entries())
		})
	}
}

var errDiskGone = errors.New("disk gone")

// TestPublishIndex_SyncFailureKeepsPreviousIndex: a stage that cannot
// be made durable is not published, and the previous index stays.
func TestPublishIndex_SyncFailureKeepsPreviousIndex(t *testing.T) {
	outDir := filepath.Join(t.TempDir(), "out")
	writeFile(t, filepath.Join(outDir, format.ManifestFile), "old")
	stage, err := newStagingDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(stage, format.ManifestFile), "new")
	failing := func(string, bool) error { return errDiskGone }

	if err := publish(nopLogger(), stage, outDir, failing); !errors.Is(err, errDiskGone) {
		t.Fatalf("publish = %v, want %v", err, errDiskGone)
	}
	if got := readFile(t, filepath.Join(outDir, format.ManifestFile)); got != "old" {
		t.Errorf("manifest = %q, want the previous index's", got)
	}
	if got := readFile(t, filepath.Join(stage, format.ManifestFile)); got != "new" {
		t.Errorf("stage manifest = %q, want it left for the caller to remove", got)
	}
}
