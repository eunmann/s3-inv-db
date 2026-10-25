//go:build integration

package s3test

import "testing"

func TestFixtureBucketName(t *testing.T) {
	tests := []struct {
		name, key, tag, want string
	}{
		{name: "untagged", key: "realistic_sorted-1000000-16", want: "fixture-realistic-sorted-1000000-16"},
		{name: "base side", key: "realistic-1000000-16", tag: "base-80987F6aa", want: "fixture-realistic-1000000-16-base-80987f6aa"},
		{name: "head side", key: "realistic-1000000-16", tag: "head", want: "fixture-realistic-1000000-16-head"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fixtureBucketName(tt.key, tt.tag); got != tt.want {
				t.Errorf("fixtureBucketName(%q, %q) = %q, want %q", tt.key, tt.tag, got, tt.want)
			}
		})
	}
}
