package loader

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestNew_StoresCacheRoot(t *testing.T) {
	root := t.TempDir()
	l := New(root, nil)
	want := filepath.Join(root, "buck", "inv", "2026-01-01T00-00Z")
	if got := l.CacheDirFor("buck", "inv", "2026-01-01T00-00Z"); got != want {
		t.Errorf("CacheDirFor = %q, want %q", got, want)
	}
}

func TestCacheDirFor_NestsBySrcIDRun(t *testing.T) {
	l := New("/cache", nil)
	cases := []struct {
		src, id, run, want string
	}{
		{"my-bucket", "inv-1", "2026-05-13T03-00Z", "/cache/my-bucket/inv-1/2026-05-13T03-00Z"},
		{"a", "b", "r", "/cache/a/b/r"},
	}
	for _, c := range cases {
		if got := l.CacheDirFor(c.src, c.id, c.run); got != c.want {
			t.Errorf("CacheDirFor(%q,%q,%q) = %q, want %q", c.src, c.id, c.run, got, c.want)
		}
	}
}

func TestBuild_RejectsEmptyArgs(t *testing.T) {
	l := New(t.TempDir(), nil)
	ctx := context.Background()
	cases := []struct {
		name                   string
		src, id, run, manifest string
		wantErr                error
	}{
		{"empty src", "", "inv", "r", "s3://b/m", errEmptyID},
		{"empty id", "buck", "", "r", "s3://b/m", errEmptyID},
		{"empty run", "buck", "inv", "", "s3://b/m", errEmptyID},
		{"empty manifest", "buck", "inv", "r", "", errEmptyManifest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := l.Build(ctx, c.src, c.id, c.run, c.manifest)
			if !errors.Is(err, c.wantErr) {
				t.Errorf("Build err = %v, want %v", err, c.wantErr)
			}
		})
	}
}

// TestBuildWith_ReportsPreparingStage exercises the loader's own stage
// reporting on the failure path (invalid manifest URI).
func TestBuildWith_ReportsPreparingStage(t *testing.T) {
	l := New(t.TempDir(), nil)
	var stages []string
	_, _ = l.BuildWith(context.Background(), "buck", "inv", "r", "not-s3-uri", func(name string, _, _ int64) {
		stages = append(stages, name)
	})
	if len(stages) == 0 || stages[0] != "preparing" {
		t.Errorf("first stage = %v, want preparing as first entry", stages)
	}
}

func TestBuildWith_NilCallbackIsSafe(t *testing.T) {
	l := New(t.TempDir(), nil)
	_, err := l.BuildWith(context.Background(), "", "inv", "r", "s3://b/m", nil)
	if !errors.Is(err, errEmptyID) {
		t.Errorf("err = %v, want errEmptyID", err)
	}
}
