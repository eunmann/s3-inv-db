package cli

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
)

// A publish build stages its index in a hidden directory under
// --work-dir holding publishLockFile, which the build keeps locked
// while it runs. The kernel drops the lock however the build exits, so
// a later build can tell a killed build's directory from one another
// build sharing --work-dir is still using.
const (
	publishDirPrefix = ".s3-inv-db-publish-"
	publishLockFile  = "publish.lock"
)

// publishDir is a publish build's work directory and the lock that
// marks it in use.
type publishDir struct {
	path string
	lock *os.File
}

// newPublishDir creates and locks a work directory under workDir.
func newPublishDir(workDir string) (*publishDir, error) {
	path, err := os.MkdirTemp(workDir, publishDirPrefix)
	if err != nil {
		return nil, fmt.Errorf("create work dir under %s: %w", workDir, err)
	}
	lock, err := createLock(path)
	if err != nil {
		if rmErr := os.RemoveAll(path); rmErr != nil {
			err = errors.Join(err, fmt.Errorf("remove work dir: %w", rmErr))
		}

		return nil, err
	}

	return &publishDir{path: path, lock: lock}, nil
}

// createLock creates publishLockFile in dir, locked. It is locked under
// another name and renamed into place, so a reclaiming build never
// finds the marker unlocked while its owner is still starting.
func createLock(dir string) (*os.File, error) {
	pending := filepath.Join(dir, publishLockFile+".new")
	f, err := os.OpenFile(pending, os.O_RDWR|os.O_CREATE|os.O_EXCL, format.FilePerm)
	if err != nil {
		return nil, fmt.Errorf("create work dir lock: %w", err)
	}
	err = lockFile(f)
	if err == nil {
		err = os.Rename(pending, filepath.Join(dir, publishLockFile))
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("lock work dir: %w", err), f.Close())
	}

	return f, nil
}

// errLockHeld is returned when a fresh lock file is already locked,
// which only a process meddling with the work directory can cause.
var errLockHeld = errors.New("lock already held")

// lockFile takes f's lock, which nothing else can hold yet.
func lockFile(f *os.File) error {
	locked, err := tryLock(f)
	if err != nil {
		return err
	}
	if !locked {
		return fmt.Errorf("%s: %w", f.Name(), errLockHeld)
	}

	return nil
}

// tryLock takes an exclusive flock on f without waiting and reports
// whether it got it.
func tryLock(f *os.File) (bool, error) {
	conn, err := f.SyscallConn()
	if err != nil {
		return false, fmt.Errorf("lock %s: %w", f.Name(), err)
	}
	var lockErr error
	if err := conn.Control(func(fd uintptr) {
		lockErr = unix.Flock(int(fd), unix.LOCK_EX|unix.LOCK_NB)
	}); err != nil {
		return false, fmt.Errorf("lock %s: %w", f.Name(), err)
	}
	if errors.Is(lockErr, unix.EWOULDBLOCK) {
		return false, nil
	}
	if lockErr != nil {
		return false, fmt.Errorf("lock %s: %w", f.Name(), lockErr)
	}

	return true, nil
}

// Close removes the directory, then releases its lock.
func (d *publishDir) Close() error {
	var errs []error
	if err := os.RemoveAll(d.path); err != nil {
		errs = append(errs, fmt.Errorf("remove work dir: %w", err))
	}
	if err := d.lock.Close(); err != nil {
		errs = append(errs, fmt.Errorf("release work dir lock: %w", err))
	}

	return errors.Join(errs...)
}

// reclaimPublishDirs removes the work directories killed publish builds
// left under workDir. A directory is removed only when it is named like
// one, holds a regular publishLockFile, and no running build holds that
// lock; anything else is left alone. A directory that can't be checked
// or removed is logged and kept.
func reclaimPublishDirs(log *zerolog.Logger, workDir string) error {
	entries, err := os.ReadDir(workDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read work dir %s: %w", workDir, err)
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), publishDirPrefix) {
			continue
		}
		path := filepath.Join(workDir, e.Name())
		removed, err := reclaimPublishDir(path)
		if err != nil {
			log.Warn().Err(err).Str("path", path).Msg("could not reclaim a publish work dir")

			continue
		}
		if removed {
			log.Warn().Str("path", path).Msg("removed a work dir left by a killed build")
		}
	}

	return nil
}

// reclaimPublishDir removes dir if it holds a publish lock that no
// running build holds, and reports whether it did.
func reclaimPublishDir(dir string) (bool, error) {
	lockPath := filepath.Join(dir, publishLockFile)
	before, err := os.Lstat(lockPath)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat lock: %w", err)
	}
	if !before.Mode().IsRegular() {
		return false, nil
	}
	f, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		return false, fmt.Errorf("open lock: %w", err)
	}
	defer f.Close()
	locked, err := tryLock(f)
	if err != nil || !locked {
		return false, err
	}
	// The lock taken must be the one at lockPath now: not a file swapped
	// in since the Lstat, nor one whose directory another build already
	// reclaimed while this one waited to open it.
	held, err := f.Stat()
	if err != nil {
		return false, fmt.Errorf("stat lock: %w", err)
	}
	now, err := os.Lstat(lockPath)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat lock: %w", err)
	}
	if !os.SameFile(before, held) || !os.SameFile(held, now) {
		return false, nil
	}
	if err := os.RemoveAll(dir); err != nil {
		return false, fmt.Errorf("remove: %w", err)
	}

	return true, nil
}
