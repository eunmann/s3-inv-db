package cli

import (
	"errors"
	"os"
	"testing"
)

var errStopAfterBuild = errors.New("stop after build")

// TestBuildPublish_BuildsTransient: an S3 publish builds its local copy
// with TransientOutput, so it is never synced to local disk; a local
// build keeps it off.
func TestBuildPublish_BuildsTransient(t *testing.T) {
	var got buildSettings
	work := t.TempDir()
	err := runBuildPublish("s3://dest/indexes/", work,
		buildSettings{s3Manifest: "s3://inv/inventory/data-bucket/daily/2026-10-24T01-00Z/manifest.json"},
		func(_ string, b buildSettings) error {
			got = b

			return errStopAfterBuild
		})
	if !errors.Is(err, errStopAfterBuild) {
		t.Fatalf("runBuildPublish = %v, want the build's error", err)
	}
	if !got.extsortConfig().TransientOutput {
		t.Error("publish build: TransientOutput = false, want true")
	}
	if left, _ := os.ReadDir(work); len(left) != 0 {
		t.Errorf("work dir left %v", left)
	}
	if (buildSettings{}).extsortConfig().TransientOutput {
		t.Error("local build: TransientOutput = true, want false")
	}
}
