package s3disco

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
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
			d := New(&s3.Client{}, "bucket", c.input)
			if got := d.Prefix(); got != c.want {
				t.Errorf("Prefix() = %q, want %q (from input %q)", got, c.want, c.input)
			}
		})
	}
}

func TestNew_BucketAndPrefixReadback(t *testing.T) {
	d := New(&s3.Client{}, "my-bucket", "data/")
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
			d, err := NewFromS3URI(&s3.Client{}, c.uri)
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
		if _, err := NewFromS3URI(&s3.Client{}, bad); err == nil {
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
			if got := trimPrefix(c.s, c.prefix); got != c.want {
				t.Errorf("trimPrefix(%q, %q) = %q, want %q", c.s, c.prefix, got, c.want)
			}
		})
	}
}

func TestRunFolderRegex_AcceptsKnownShapes(t *testing.T) {
	for _, ok := range []string{"2026-05-13T03-02Z", "2026-05-13T03-02-15Z"} {
		if !runFolderRE.MatchString(ok) {
			t.Errorf("runFolderRE should accept %q", ok)
		}
	}
	for _, bad := range []string{"data", "data/", "manifest.json", "2026-05-13", "garbage", ""} {
		if runFolderRE.MatchString(bad) {
			t.Errorf("runFolderRE should reject %q", bad)
		}
	}
}
