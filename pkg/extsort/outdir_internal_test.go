package extsort

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
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
