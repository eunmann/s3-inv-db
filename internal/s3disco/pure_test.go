package s3disco_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/s3disco"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

func TestNew_NormalizesPrefixTrailingSlash(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"", ""},
		{"foo/", "foo/"},
		{"foo", "foo/"},
		{"nested/path/", "nested/path/"},
		{"nested/path", "nested/path/"},
	}
	for _, c := range cases {
		t.Run(c.input, func(t *testing.T) {
			d := s3disco.New(&s3.Client{}, "bucket", c.input)
			if got := d.Prefix(); got != c.want {
				t.Errorf("Prefix() = %q, want %q (from input %q)", got, c.want, c.input)
			}
		})
	}
}

func TestNew_BucketAndPrefixReadback(t *testing.T) {
	d := s3disco.New(&s3.Client{}, "my-bucket", "data/")
	if got := d.Bucket(); got != "my-bucket" {
		t.Errorf("Bucket() = %q, want %q", got, "my-bucket")
	}
	if got := d.Prefix(); got != "data/" {
		t.Errorf("Prefix() = %q, want %q", got, "data/")
	}
}

func TestNewFromS3URI_ParsesBucketAndPrefix(t *testing.T) {
	cases := []struct {
		uri        string
		wantBucket string
		wantPrefix string
	}{
		{"s3://b/data/", "b", "data/"},
		{"s3://b/", "b", ""},
		{"s3://my-bucket/inventory-data/", "my-bucket", "inventory-data/"},
	}
	for _, c := range cases {
		t.Run(c.uri, func(t *testing.T) {
			d, err := s3disco.NewFromS3URI(&s3.Client{}, c.uri)
			if err != nil {
				t.Fatalf("NewFromS3URI: %v", err)
			}
			if d.Bucket() != c.wantBucket {
				t.Errorf("Bucket() = %q, want %q", d.Bucket(), c.wantBucket)
			}
			if d.Prefix() != c.wantPrefix {
				t.Errorf("Prefix() = %q, want %q", d.Prefix(), c.wantPrefix)
			}
		})
	}
}

func TestNewFromS3URI_RejectsMalformedURI(t *testing.T) {
	for _, bad := range []string{"", "not-a-uri", "http://b/k", "s3://"} {
		if _, err := s3disco.NewFromS3URI(&s3.Client{}, bad); err == nil {
			t.Errorf("NewFromS3URI(%q) returned nil error, want a parse error", bad)
		}
	}
}

func TestTrimPrefix_StripsBothEnds(t *testing.T) {
	cases := []struct {
		s, prefix, want string
	}{
		{"data/src/", "data/", "src"},
		{"data/src", "data/", "src"},
		{"src/", "", "src"},
		{"data/nested/path/", "data/", "nested/path"},
		{"different/path/", "data/", "different/path"},
	}
	for _, c := range cases {
		t.Run(c.s, func(t *testing.T) {
			if got := s3disco.TrimPrefix(c.s, c.prefix); got != c.want {
				t.Errorf("trimPrefix(%q, %q) = %q, want %q", c.s, c.prefix, got, c.want)
			}
		})
	}
}

func TestManifestTotalBytes(t *testing.T) {
	cases := []struct {
		name  string
		files []s3fetch.ManifestFile
		want  int64
	}{
		{name: "empty", files: nil, want: 0},
		{name: "single", files: []s3fetch.ManifestFile{{Size: 42}}, want: 42},
		{
			name:  "multiple",
			files: []s3fetch.ManifestFile{{Size: 100}, {Size: 250}, {Size: 9999}},
			want:  10349,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := &s3fetch.Manifest{Files: c.files}
			if got := s3disco.ManifestTotalBytes(m); got != c.want {
				t.Errorf("ManifestTotalBytes = %d, want %d", got, c.want)
			}
		})
	}
}

func TestRunFolderRegex_AcceptsKnownShapes(t *testing.T) {
	for _, ok := range []string{"2026-05-13T03-02Z", "2026-05-13T03-02-15Z"} {
		if !s3disco.RunFolderRE.MatchString(ok) {
			t.Errorf("runFolderRE should accept %q", ok)
		}
	}
	for _, bad := range []string{"data", "data/", "manifest.json", "2026-05-13", "garbage", ""} {
		if s3disco.RunFolderRE.MatchString(bad) {
			t.Errorf("runFolderRE should reject %q", bad)
		}
	}
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

func TestValidSourceName(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
	}{
		{"src", true},
		{"my-bucket.logs", true},
		{"synthetic-prod", true},
		{"a1b", true},
		{strings.Repeat("a", 63), true},
		{"ab", false},
		{strings.Repeat("a", 64), false},
		{"", false},
		{"..", false},
		{"...", false},
		{"-bucket", false},
		{"bucket-", false},
		{"Bucket", false},
		{"my_bucket", false},
		{"a/b/c", false},
		{"row[alert(1)]", false},
		{"a b", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := s3disco.ValidSourceName(c.name); got != c.ok {
				t.Errorf("ValidSourceName(%q) = %v, want %v", c.name, got, c.ok)
			}
		})
	}
}

func TestValidInventoryID(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
	}{
		{"inv-001", true},
		{"Daily_Inventory.v2", true},
		{"x", true},
		{strings.Repeat("a", 64), true},
		{"a..b", true},
		{"", false},
		{".", false},
		{"..", false},
		{strings.Repeat("a", 65), false},
		{"a/b", false},
		{`a\b`, false},
		{"inv[x]", false},
		{`inv"x`, false},
		{"inv x", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := s3disco.ValidInventoryID(c.name); got != c.ok {
				t.Errorf("ValidInventoryID(%q) = %v, want %v", c.name, got, c.ok)
			}
		})
	}
}

func TestFind_RejectsInvalidNamesWithoutListing(t *testing.T) {
	// A zero s3.Client panics if used, so a passing test proves the
	// names are rejected before any S3 call.
	d := s3disco.New(&s3.Client{}, "bucket", "data/")
	for _, c := range []struct{ src, inv string }{
		{"..", "inv-001"},
		{"src", ".."},
		{"src", "inv[alert(1)]"},
		{"Src_Bad", "inv-001"},
	} {
		if _, err := d.Find(t.Context(), c.src, c.inv, ""); !errors.Is(err, inventory.ErrInvalidName) {
			t.Errorf("Find(%q, %q) err = %v, want ErrInvalidName", c.src, c.inv, err)
		}
	}
}
