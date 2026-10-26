package format

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
)

// checksumPiece is the size of the pieces a file's checksum hashes
// separately, so a large file is hashed on every core rather than one.
const checksumPiece = 64 << 20

// checksumBuffer is the read size within a piece.
const checksumBuffer = 1 << 20

// pieceJob is one piece of one file to hash.
type pieceJob struct {
	name   string
	off, n int64
	digest *[sha256.Size]byte
}

// checksumFiles returns the checksum of each file under dir that sizes
// names, given its size: the hex SHA-256 of the concatenated SHA-256
// digests of the file's checksumPiece-sized pieces (one empty piece for
// an empty file). Every piece of every file is hashed in parallel on up
// to GOMAXPROCS workers, which keeps a build's checksums from being one
// thread reading the whole index; hashed counts the bytes read.
func checksumFiles(dir string, sizes map[string]int64, hashed *stepCounter) (map[string]string, error) {
	digests := make(map[string][][sha256.Size]byte, len(sizes))
	var jobs []pieceJob
	for name, size := range sizes {
		pieces := max(1, (size+checksumPiece-1)/checksumPiece)
		d := make([][sha256.Size]byte, pieces)
		digests[name] = d
		for i := range pieces {
			off := i * checksumPiece
			jobs = append(jobs, pieceJob{name: name, off: off, n: min(checksumPiece, size-off), digest: &d[i]})
		}
	}

	var next atomic.Int64
	workers := min(runtime.GOMAXPROCS(0), len(jobs))
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			buf := make([]byte, checksumBuffer)
			for j := next.Add(1) - 1; j < int64(len(jobs)); j = next.Add(1) - 1 {
				if err := hashPiece(dir, jobs[j], buf, hashed); err != nil {
					errs[w] = err
					next.Store(int64(len(jobs))) // stop the other workers

					return
				}
			}
		})
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	sums := make(map[string]string, len(digests))
	for name, d := range digests {
		h := sha256.New()
		for i := range d {
			h.Write(d[i][:])
		}
		sums[name] = hex.EncodeToString(h.Sum(nil))
	}

	return sums, nil
}

// hashPiece hashes job's piece of its file into job.digest. A file
// shorter than the piece is an error: its size changed since it was
// listed.
func hashPiece(dir string, job pieceJob, buf []byte, hashed *stepCounter) error {
	f, err := os.Open(filepath.Join(dir, job.name))
	if err != nil {
		return fmt.Errorf("checksum %s: %w", job.name, err)
	}
	defer f.Close()
	h := sha256.New()
	r := io.NewSectionReader(f, job.off, job.n)
	var read int64
	for {
		n, err := r.Read(buf)
		h.Write(buf[:n])
		hashed.add(n)
		read += int64(n)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("checksum %s: %w", job.name, err)
		}
	}
	if read != job.n {
		return fmt.Errorf("checksum %s: %w", job.name, ErrSizeMismatch)
	}
	h.Sum(job.digest[:0])

	return nil
}
