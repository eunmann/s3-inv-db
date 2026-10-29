package cli

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// abandon simulates the build owning d being killed: its lock is
// released, as the kernel does on exit, and its directory stays.
func abandon(t *testing.T, d *publishDir) {
	t.Helper()
	if err := d.lock.Close(); err != nil {
		t.Fatal(err)
	}
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Lstat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}

	return err == nil
}

// TestReclaimPublishDirs: a publish dir whose build is gone is removed;
// one a running build holds is kept, as is anything under --work-dir
// this package did not create, however much it looks like a publish dir.
func TestReclaimPublishDirs(t *testing.T) {
	work := t.TempDir()

	stale, err := newPublishDir(work)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(stale.path, "index"), 0o750); err != nil {
		t.Fatal(err)
	}
	abandon(t, stale)

	live, err := newPublishDir(work)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()

	noMarker := filepath.Join(work, publishDirPrefix+"user")
	if err := os.MkdirAll(filepath.Join(noMarker, "index"), 0o750); err != nil {
		t.Fatal(err)
	}
	otherName := filepath.Join(work, "s3-inv-db-publish-visible")
	if err := os.Mkdir(otherName, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(otherName, publishLockFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	plainFile := filepath.Join(work, publishDirPrefix+"file")
	if err := os.WriteFile(plainFile, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, publishLockFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(work, publishDirPrefix+"link")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	lockLink := filepath.Join(work, publishDirPrefix+"locklink")
	if err := os.Mkdir(lockLink, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(target, publishLockFile), filepath.Join(lockLink, publishLockFile)); err != nil {
		t.Fatal(err)
	}

	log := zerolog.Nop()
	if err := reclaimPublishDirs(&log, work); err != nil {
		t.Fatalf("reclaimPublishDirs: %v", err)
	}

	if exists(t, stale.path) {
		t.Errorf("abandoned publish dir %s kept", stale.path)
	}
	for _, kept := range []string{live.path, noMarker, otherName, plainFile, symlink, lockLink, filepath.Join(target, publishLockFile)} {
		if !exists(t, kept) {
			t.Errorf("%s removed, want kept", kept)
		}
	}
}

// TestBuildPublish_ReclaimsAbandonedDirs: a publish build removes what
// a killed earlier build left under --work-dir before it starts its own.
func TestBuildPublish_ReclaimsAbandonedDirs(t *testing.T) {
	work := t.TempDir()
	stale, err := newPublishDir(work)
	if err != nil {
		t.Fatal(err)
	}
	abandon(t, stale)

	staleAtBuild := true
	err = runBuildPublish("s3://dest/indexes/", work,
		buildSettings{s3Manifest: "s3://inv/inventory/data-bucket/daily/2026-10-24T01-00Z/manifest.json", baseLogger: zerolog.Nop()},
		func(string, buildSettings) error {
			staleAtBuild = exists(t, stale.path)

			return errStopAfterBuild
		})
	if !errors.Is(err, errStopAfterBuild) {
		t.Fatalf("runBuildPublish = %v, want the build's error", err)
	}
	if staleAtBuild {
		t.Errorf("abandoned publish dir %s still there when the build ran", stale.path)
	}
}

// TestPublishDir_PartialRemovalKeepsMarker: when a publish dir can only
// be partly removed, its lock file stays, so a later build still
// reclaims what is left once removal can succeed.
func TestPublishDir_PartialRemovalKeepsMarker(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	work := t.TempDir()
	d, err := newPublishDir(work)
	if err != nil {
		t.Fatal(err)
	}
	stuck := filepath.Join(d.path, "index")
	if err := os.Mkdir(stuck, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stuck, "data"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(stuck)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(stuck, 0o400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(stuck, info.Mode().Perm()) })

	if err := d.Close(); err == nil {
		t.Fatal("Close succeeded on a dir it could not empty")
	}
	if !exists(t, filepath.Join(d.path, publishLockFile)) {
		t.Fatal("lock file removed from a dir that is still there")
	}

	log := zerolog.Nop()
	if err := reclaimPublishDirs(&log, work); err != nil {
		t.Fatalf("reclaimPublishDirs: %v", err)
	}
	if !exists(t, filepath.Join(d.path, publishLockFile)) {
		t.Fatal("failed reclaim removed the lock file")
	}

	if err := os.Chmod(stuck, info.Mode().Perm()); err != nil {
		t.Fatal(err)
	}
	if err := reclaimPublishDirs(&log, work); err != nil {
		t.Fatalf("reclaimPublishDirs: %v", err)
	}
	if exists(t, d.path) {
		t.Errorf("publish dir %s kept after removal could succeed", d.path)
	}
}
