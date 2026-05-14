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

// TestBuildWith_ReportsPreparingStage exercises the loader's own stage
// reporting on the failure path (invalid manifest URI). The pipeline
// phases need real S3 data to fire, so they're tested through the
// integration path; the loader-level stage names are what we pin here.
func TestBuildWith_ReportsPreparingStage(t *testing.T) {
	l := New(t.TempDir(), nil)
	var stages []string
	_, _ = l.BuildWith(context.Background(), "buck", "inv", "not-s3-uri", func(name string, _, _ int64) {
		stages = append(stages, name)
	})
	if len(stages) == 0 || stages[0] != "preparing" {
		t.Errorf("first stage = %v, want preparing as first entry", stages)
	}
}

func TestBuildWith_NilCallbackIsSafe(t *testing.T) {
	l := New(t.TempDir(), nil)
	_, err := l.BuildWith(context.Background(), "", "inv", "s3://b/m", nil)
	if !errors.Is(err, errEmptyID) {
		t.Errorf("err = %v, want errEmptyID", err)
	}
}
