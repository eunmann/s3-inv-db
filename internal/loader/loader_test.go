package loader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestNew_StoresCacheRoot(t *testing.T) {
	root := t.TempDir()
	l := New(root, nil)
	if got := l.CacheDirFor("buck", "inv"); got != filepath.Join(root, "buck", "inv") {
		t.Errorf("CacheDirFor = %q, want %q", got, filepath.Join(root, "buck", "inv"))
	}
}

func TestCacheDirFor_NestsBySrcAndID(t *testing.T) {
	l := New("/cache", nil)
	cases := []struct {
		src, id, want string
	}{
		{"my-bucket", "inv-1", "/cache/my-bucket/inv-1"},
		{"a", "b", "/cache/a/b"},
	}
	for _, c := range cases {
		if got := l.CacheDirFor(c.src, c.id); got != c.want {
			t.Errorf("CacheDirFor(%q,%q) = %q, want %q", c.src, c.id, got, c.want)
		}
	}
}

func TestBuild_RejectsEmptyArgs(t *testing.T) {
	l := New(t.TempDir(), nil)
	ctx := context.Background()
	cases := []struct {
		name              string
		src, id, manifest string
		wantErr           error
	}{
		{"empty src", "", "inv", "s3://b/m", errEmptyID},
		{"empty id", "buck", "", "s3://b/m", errEmptyID},
		{"empty manifest", "buck", "inv", "", errEmptyManifest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := l.Build(ctx, c.src, c.id, c.manifest)
			if !errors.Is(err, c.wantErr) {
				t.Errorf("Build err = %v, want %v", err, c.wantErr)
			}
		})
	}
}

func TestEvict_RemovesCacheDir(t *testing.T) {
	root := t.TempDir()
	l := New(root, nil)

	dir := l.CacheDirFor("buck", "inv")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Drop a file in there to make sure RemoveAll handles non-empty dirs.
	if err := os.WriteFile(filepath.Join(dir, "marker"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}

	if err := l.Evict("buck", "inv"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("cache dir still exists after Evict: stat err = %v", err)
	}
}

func TestEvict_NoopOnMissingDir(t *testing.T) {
	root := t.TempDir()
	l := New(root, nil)
	// Inventory was never built; Evict must succeed regardless.
	if err := l.Evict("never", "built"); err != nil {
		t.Errorf("Evict on missing dir returned: %v", err)
	}
}

func TestEvict_OnlyTouchesNamedSubtree(t *testing.T) {
	// Sanity: evicting one inventory must not affect siblings under the
	// same src bucket or other src buckets.
	root := t.TempDir()
	l := New(root, nil)

	mk := func(src, id string) string {
		p := l.CacheDirFor(src, id)
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", p, err)
		}
		return p
	}
	target := mk("buck", "inv-1")
	siblingSameSrc := mk("buck", "inv-2")
	otherSrc := mk("otherbuck", "inv-1")

	if err := l.Evict("buck", "inv-1"); err != nil {
		t.Fatalf("Evict: %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Errorf("target dir still exists: %v", err)
	}
	if _, err := os.Stat(siblingSameSrc); err != nil {
		t.Errorf("sibling dir affected: %v", err)
	}
	if _, err := os.Stat(otherSrc); err != nil {
		t.Errorf("other-bucket dir affected: %v", err)
	}
}
