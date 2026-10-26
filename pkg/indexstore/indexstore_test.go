package indexstore_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexstore"
)

func TestRunFromManifestKey(t *testing.T) {
	r, err := indexstore.RunFromManifestKey("inventory/data-bucket/daily/2026-10-24T01-00Z/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	want := indexstore.Run{Source: "data-bucket", Inventory: "daily", Run: "2026-10-24T01-00Z"}
	if r != want {
		t.Fatalf("run = %+v, want %+v", r, want)
	}
	if got := r.Prefix("indexes/"); got != "indexes/data-bucket/daily/2026-10-24T01-00Z/" {
		t.Errorf("Prefix = %q", got)
	}
	if got := r.Prefix(""); got != "data-bucket/daily/2026-10-24T01-00Z/" {
		t.Errorf("Prefix at the bucket root = %q", got)
	}
	for _, key := range []string{"daily/2026/manifest.json", "a/b/c/d/other.json", "x/../y/z/manifest.json", "a//c/manifest.json"} {
		if _, err := indexstore.RunFromManifestKey(key); !errors.Is(err, indexstore.ErrNotRunManifest) {
			t.Errorf("RunFromManifestKey(%q) err = %v, want ErrNotRunManifest", key, err)
		}
	}
}

// TestParseManifestRejectsPathsOutsideTheRun: a manifest comes from S3,
// so a file name that would be written outside the run's directory is
// refused before anything is downloaded.
func TestParseManifestRejectsPathsOutsideTheRun(t *testing.T) {
	if _, err := indexstore.ParseManifest([]byte(`{"version":2,"files":{"core_stats.bin":{"size":1},"tier_stats/tier_multi.bin":{"size":2}}}`)); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
	for _, name := range []string{"../x", "/etc/passwd", "a/../../b", "a/./b", `a\b`, "", "manifest.json"} {
		data := []byte(`{"files":{"` + jsonEscape(name) + `":{"size":1}}}`)
		if _, err := indexstore.ParseManifest(data); !errors.Is(err, indexstore.ErrBadManifest) {
			t.Errorf("name %q: err = %v, want ErrBadManifest", name, err)
		}
	}
	if _, err := indexstore.ParseManifest([]byte(`{"files":{}}`)); !errors.Is(err, indexstore.ErrBadManifest) {
		t.Errorf("empty manifest: err = %v, want ErrBadManifest", err)
	}
}

// TestParseManifestRejectsOtherVersions: a run published under another
// manifest version is refused before anything is downloaded, as
// unpublished, so a loader that may build rebuilds it.
func TestParseManifestRejectsOtherVersions(t *testing.T) {
	for _, v := range []int{format.ManifestVersion - 1, format.ManifestVersion + 1} {
		data := fmt.Appendf(nil, `{"version":%d,"files":{"core_stats.bin":{"size":1}}}`, v)
		_, err := indexstore.ParseManifest(data)
		if !errors.Is(err, indexstore.ErrNotPublished) || !errors.Is(err, format.ErrManifestVersion) {
			t.Errorf("version %d: err = %v, want ErrNotPublished and ErrManifestVersion", v, err)
		}
	}
}

func jsonEscape(s string) string {
	out := make([]byte, 0, len(s))
	for i := range len(s) {
		if s[i] == '\\' || s[i] == '"' {
			out = append(out, '\\')
		}
		out = append(out, s[i])
	}

	return string(out)
}
