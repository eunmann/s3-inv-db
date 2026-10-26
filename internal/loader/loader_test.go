package loader_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loader"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
)

func key(src, id, run string) inventory.CacheKey {
	return inventory.CacheKey{SourceBucket: src, InventoryID: id, Run: run}
}

func TestNew_StoresCacheRoot(t *testing.T) {
	root := t.TempDir()
	l := loader.New(root, nil)
	want := filepath.Join(root, "buck", "inv", "2026-01-01T00-00Z")
	got, err := l.CacheDirFor(key("buck", "inv", "2026-01-01T00-00Z"))
	if err != nil {
		t.Fatalf("CacheDirFor: %v", err)
	}
	if got != want {
		t.Errorf("CacheDirFor = %q, want %q", got, want)
	}
}

func TestCacheDirFor_NestsBySrcIDRun(t *testing.T) {
	l := loader.New("/cache", nil)
	cases := []struct {
		src, id, run, want string
	}{
		{"my-bucket", "inv-1", "2026-05-13T03-00Z", "/cache/my-bucket/inv-1/2026-05-13T03-00Z"},
		{"a", "b", "r", "/cache/a/b/r"},
	}
	for _, c := range cases {
		got, err := l.CacheDirFor(key(c.src, c.id, c.run))
		if err != nil {
			t.Fatalf("CacheDirFor(%q,%q,%q): %v", c.src, c.id, c.run, err)
		}
		if got != c.want {
			t.Errorf("CacheDirFor(%q,%q,%q) = %q, want %q", c.src, c.id, c.run, got, c.want)
		}
	}
}

// unsafeKeys are cache keys whose naive filepath.Join would land outside
// <root>/<src>/<id>/ — at the root itself, a sibling, or above it.
func unsafeKeys() map[string]inventory.CacheKey {
	return map[string]inventory.CacheKey{
		"dotdot src":        key("..", "inv", "r"),
		"dotdot id":         key("buck", "..", "r"),
		"dotdot run":        key("buck", "inv", ".."),
		"collapse to root":  key("buck", "..", ".."),
		"dot segment":       key("buck", ".", "r"),
		"nested escape":     key("buck/../..", "inv", "r"),
		"slash in id":       key("buck", "a/b", "r"),
		"absolute src":      key("/etc", "inv", "r"),
		"backslash in name": key("buck", `a\b`, "r"),
	}
}

func TestCacheDirFor_RefusesPathsOutsideRoot(t *testing.T) {
	l := loader.New(t.TempDir(), nil)
	for name, k := range unsafeKeys() {
		t.Run(name, func(t *testing.T) {
			if dir, err := l.CacheDirFor(k); !errors.Is(err, loader.ErrUnsafeCacheKey) {
				t.Errorf("CacheDirFor(%+v) = (%q, %v), want ErrUnsafeCacheKey", k, dir, err)
			}
		})
	}
}

func TestRemoveCache_RefusesPathsOutsideRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "cache")
	sentinel := filepath.Join(parent, "keep.txt")
	if err := os.MkdirAll(filepath.Join(root, "buck", "r"), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(sentinel, []byte("x"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	l := loader.New(root, nil)
	for name, k := range unsafeKeys() {
		t.Run(name, func(t *testing.T) {
			if err := l.RemoveCache(k); !errors.Is(err, loader.ErrUnsafeCacheKey) {
				t.Errorf("RemoveCache(%+v) = %v, want ErrUnsafeCacheKey", k, err)
			}
		})
	}
	for _, p := range []string{root, filepath.Join(root, "buck", "r"), sentinel} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s removed by an unsafe RemoveCache: %v", p, err)
		}
	}
}

func TestBuildWith_RefusesPathsOutsideRoot(t *testing.T) {
	l := loader.New(t.TempDir(), nil)
	for name, k := range unsafeKeys() {
		t.Run(name, func(t *testing.T) {
			if _, err := l.BuildWith(t.Context(), k, "s3://b/m", nil, nil); !errors.Is(err, loader.ErrUnsafeCacheKey) {
				t.Errorf("BuildWith(%+v) err = %v, want ErrUnsafeCacheKey", k, err)
			}
		})
	}
}

func TestCacheSizeBytes_RefusesPathsOutsideRoot(t *testing.T) {
	l := loader.New(t.TempDir(), nil)
	if _, err := l.CacheSizeBytes(key("buck", "..", "..")); !errors.Is(err, loader.ErrUnsafeCacheKey) {
		t.Errorf("CacheSizeBytes err = %v, want ErrUnsafeCacheKey", err)
	}
}

func TestBuild_RejectsEmptyArgs(t *testing.T) {
	l := loader.New(t.TempDir(), nil)
	ctx := t.Context()
	cases := []struct {
		wantErr  error
		name     string
		src      string
		id       string
		run      string
		manifest string
	}{
		{name: "empty src", src: "", id: "inv", run: "r", manifest: "s3://b/m", wantErr: loader.ErrEmptyID},
		{name: "empty id", src: "buck", id: "", run: "r", manifest: "s3://b/m", wantErr: loader.ErrEmptyID},
		{name: "empty run", src: "buck", id: "inv", run: "", manifest: "s3://b/m", wantErr: loader.ErrEmptyID},
		{name: "empty manifest", src: "buck", id: "inv", run: "r", manifest: "", wantErr: loader.ErrEmptyManifest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := l.BuildWith(ctx, key(c.src, c.id, c.run), c.manifest, nil, nil)
			if !errors.Is(err, c.wantErr) {
				t.Errorf("Build err = %v, want %v", err, c.wantErr)
			}
		})
	}
}

func TestBuildWith_ReportsPreparingStage(t *testing.T) {
	l := loader.New(t.TempDir(), nil)
	var stages []string
	_, _ = l.BuildWith(t.Context(), key("buck", "inv", "r"), "not-s3-uri", func(p events.Progress) {
		stages = append(stages, p.Stage)
	}, nil)
	if len(stages) == 0 || stages[0] != "preparing" {
		t.Errorf("first stage = %v, want preparing as first entry", stages)
	}
}

func TestBuildWith_NilCallbackIsSafe(t *testing.T) {
	l := loader.New(t.TempDir(), nil)
	_, err := l.BuildWith(t.Context(), key("", "inv", "r"), "s3://b/m", nil, nil)
	if !errors.Is(err, loader.ErrEmptyID) {
		t.Errorf("err = %v, want errEmptyID", err)
	}
}

func TestRemoveCache_RejectsEmptyArgs(t *testing.T) {
	l := loader.New(t.TempDir(), nil)
	cases := []struct {
		name         string
		src, id, run string
	}{
		{"empty src", "", "inv", "r"},
		{"empty id", "buck", "", "r"},
		{"empty run", "buck", "inv", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := l.RemoveCache(key(c.src, c.id, c.run)); !errors.Is(err, loader.ErrEmptyID) {
				t.Errorf("err = %v, want errEmptyID", err)
			}
		})
	}
}

func TestRemoveCache_MissingDirIsNoOp(t *testing.T) {
	l := loader.New(t.TempDir(), nil)
	if err := l.RemoveCache(key("buck", "inv", "never-built")); err != nil {
		t.Errorf("RemoveCache(missing) = %v, want nil", err)
	}
}

func TestRemoveCache_DeletesExistingDir(t *testing.T) {
	root := t.TempDir()
	l := loader.New(root, nil)
	dir, err := l.CacheDirFor(key("buck", "inv", "r"))
	if err != nil {
		t.Fatalf("CacheDirFor: %v", err)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data.bin"), []byte("xxx"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := l.RemoveCache(key("buck", "inv", "r")); err != nil {
		t.Fatalf("RemoveCache: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("dir still exists after RemoveCache: stat err = %v", err)
	}
}

func TestCacheSizeBytes_MissingDirReturnsZero(t *testing.T) {
	l := loader.New(t.TempDir(), nil)
	size, err := l.CacheSizeBytes(key("buck", "inv", "never-built"))
	if err != nil {
		t.Fatalf("CacheSizeBytes: %v", err)
	}
	if size != 0 {
		t.Errorf("size = %d, want 0", size)
	}
}

func TestCacheSizeBytes_SumsAllFiles(t *testing.T) {
	root := t.TempDir()
	l := loader.New(root, nil)
	dir, err := l.CacheDirFor(key("buck", "inv", "r"))
	if err != nil {
		t.Fatalf("CacheDirFor: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "nested"), 0o750); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.bin"), make([]byte, 100), 0o600); err != nil {
		t.Fatalf("write a: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nested", "b.bin"), make([]byte, 250), 0o600); err != nil {
		t.Fatalf("write b: %v", err)
	}
	size, err := l.CacheSizeBytes(key("buck", "inv", "r"))
	if err != nil {
		t.Fatalf("CacheSizeBytes: %v", err)
	}
	if size != 350 {
		t.Errorf("size = %d, want 350", size)
	}
}
