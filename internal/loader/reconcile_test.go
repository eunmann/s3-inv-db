package loader_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/rs/zerolog"
)

// writeIndexDir makes dir look like an index this project wrote: a
// manifest listing the core stats file.
func writeIndexDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, format.DirPerm); err != nil {
		t.Fatal(err)
	}
	m := format.Manifest{Version: format.ManifestVersion, Files: map[string]format.FileInfo{format.CoreStatsFile: {}}}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, format.ManifestFile), data, format.FilePerm); err != nil {
		t.Fatal(err)
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)

	return err == nil
}

// TestReconcile_RemovesOnlyWhatTheServerOwns: orphaned indexes and
// staging and scratch leftovers go; the kept run, a directory that
// isn't an index, one with an unrelated manifest, and a stray file
// stay; a previous index moved aside by a killed build is restored,
// not lost.
func TestReconcile_RemovesOnlyWhatTheServerOwns(t *testing.T) {
	root := t.TempDir()
	inv := filepath.Join(root, "src", "inv")
	writeIndexDir(t, filepath.Join(inv, "kept"))
	writeIndexDir(t, filepath.Join(inv, "orphan"))
	if err := os.MkdirAll(filepath.Join(inv, "userdata"), format.DirPerm); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inv, "userdata", "notes.txt"), []byte("x"), format.FilePerm); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(inv, "other"), format.DirPerm); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inv, "other", format.ManifestFile), []byte(`{"name":"not ours"}`), format.FilePerm); err != nil {
		t.Fatal(err)
	}
	// A build of "kept" killed mid-way left a staging dir; a build of
	// "moved" was killed after moving its previous index aside; a build
	// of "fresh" was killed while sorting, leaving only its scratch.
	writeIndexDir(t, filepath.Join(inv, ".kept.building-123"))
	writeIndexDir(t, filepath.Join(inv, ".moved.replaced-456"))
	if err := os.MkdirAll(filepath.Join(inv, ".fresh.scratch-789"), format.DirPerm); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inv, ".fresh.scratch-789", "run-0001.zst"), []byte("spill"), format.FilePerm); err != nil {
		t.Fatal(err)
	}

	l := loader.New(root, nil)
	logger := zerolog.Nop()
	removed, err := l.Reconcile(&logger, func(k inventory.CacheKey) bool {
		return k.Run == "kept" || k.Run == "moved"
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if want := []string{filepath.Join(inv, "orphan")}; !slices.Equal(removed, want) {
		t.Errorf("removed = %v, want %v", removed, want)
	}
	for _, keep := range []string{"kept", "userdata", "other", "moved"} {
		if !exists(filepath.Join(inv, keep)) {
			t.Errorf("%s was removed", keep)
		}
	}
	for _, gone := range []string{"orphan", ".kept.building-123", ".moved.replaced-456", ".fresh.scratch-789"} {
		if exists(filepath.Join(inv, gone)) {
			t.Errorf("%s survived", gone)
		}
	}
	if !exists(filepath.Join(inv, "moved", format.ManifestFile)) {
		t.Error("the index moved aside by a killed build wasn't restored")
	}
}

// TestBuildWith_ClearsPreviousIndex: a rebuild clears the run's cache
// dir first, so a previous index never holds disk the budget doesn't
// count, even when the rebuild then fails.
func TestBuildWith_ClearsPreviousIndex(t *testing.T) {
	l := loader.New(t.TempDir(), nil)
	dir, err := l.CacheDirFor(key("buck", "inv", "r"))
	if err != nil {
		t.Fatal(err)
	}
	writeIndexDir(t, dir)
	if _, err := l.BuildWith(t.Context(), key("buck", "inv", "r"), "not-s3-uri", nil, nil); err == nil {
		t.Fatal("BuildWith with a bad manifest URI succeeded")
	}
	if exists(filepath.Join(dir, format.ManifestFile)) {
		t.Error("BuildWith kept the run's previous index")
	}
}
