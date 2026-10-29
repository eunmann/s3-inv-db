package benchutil

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const diskPeakEnv = "S3INV_DISK_PEAK"

// DiskPeakEnabled reports whether benchmarks should sample disk use.
// Off by default: the walks allocate, which a benchmark's allocs/op
// would count against the code under test.
func DiskPeakEnabled() bool {
	return os.Getenv(diskPeakEnv) != ""
}

// DiskPeakSampler polls the disk space a directory tree takes and keeps
// the largest total seen between Start and Stop: a build's peak of
// scratch plus index, which no single moment after the build shows.
type DiskPeakSampler struct {
	dir     string
	err     error
	done    chan struct{}
	stopped chan struct{}
	mu      sync.Mutex
	max     int64
}

const diskSamplerInterval = 10 * time.Millisecond

// StartDiskPeakSampler begins sampling dir in a background goroutine.
func StartDiskPeakSampler(dir string) *DiskPeakSampler {
	s := &DiskPeakSampler{
		dir:     dir,
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go s.run()

	return s
}

func (s *DiskPeakSampler) run() {
	defer close(s.stopped)
	t := time.NewTicker(diskSamplerInterval)
	defer t.Stop()
	for {
		s.sample()
		select {
		case <-s.done:
			return
		case <-t.C:
		}
	}
}

func (s *DiskPeakSampler) sample() {
	n, err := DiskUsage(s.dir)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil && s.err == nil {
		s.err = err
	}
	s.max = max(s.max, n)
}

// Stop ends sampling, takes a last sample and returns the peak bytes,
// or the first error a walk hit.
func (s *DiskPeakSampler) Stop() (int64, error) {
	close(s.done)
	<-s.stopped
	s.sample()
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.max, s.err
}

// DiskUsage returns the bytes the regular files under dir take on disk.
// Files removed while it walks are skipped: a build deletes scratch as
// it goes.
func DiskUsage(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}

			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("stat: %w", err)
		}
		total += allocatedBytes(info)

		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("disk usage of %s: %w", dir, err)
	}

	return total, nil
}
