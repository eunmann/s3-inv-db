package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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

// castagnoli is the CRC-32C table; hash/crc32 builds it once and
// computes it with the CPU's CRC32 instructions on amd64 and arm64.
//
// A checksum is CRC-32C, not a cryptographic hash: it is for integrity
// only. The manifest sits beside the files it covers, on disk and under
// the same S3 prefix, so whoever can change a file can change its
// checksum too; what it catches is accidental damage (a torn copy, a
// bad disk or transfer), which CRC-32C detects.
func castagnoli() *crc32.Table { return crc32.MakeTable(crc32.Castagnoli) }

// pieceJob is one piece of one file to hash.
type pieceJob struct {
	digest *uint32
	name   string
	off, n int64
}

// checksumFiles returns the checksum of each file under dir that sizes
// names, given its size: the CRC-32C of the CRC-32Cs of the file's
// piece-sized pieces (one empty piece for an empty file), each 4 bytes
// big-endian, concatenated in order, as 8 hex digits. Manifests use
// checksumPiece; tests pass a small piece. Every piece of every file
// is hashed in parallel on up to GOMAXPROCS workers, which keeps a
// build's checksums from being one thread reading the whole index;
// hashed counts the bytes read.
func checksumFiles(dir string, sizes map[string]int64, piece int64, hashed *stepCounter) (map[string]string, error) {
	digests := make(map[string][]uint32, len(sizes))
	var jobs []pieceJob
	for name, size := range sizes {
		pieces := max(1, (size+piece-1)/piece)
		d := make([]uint32, pieces)
		digests[name] = d
		for i := range pieces {
			off := i * piece
			jobs = append(jobs, pieceJob{name: name, off: off, n: min(piece, size-off), digest: &d[i]})
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

	table := castagnoli()
	sums := make(map[string]string, len(digests))
	for name, d := range digests {
		var crc uint32
		var b [4]byte
		for _, piece := range d {
			binary.BigEndian.PutUint32(b[:], piece)
			crc = crc32.Update(crc, table, b[:])
		}
		sums[name] = fmt.Sprintf("%08x", crc)
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
	table := castagnoli()
	var crc uint32
	r := io.NewSectionReader(f, job.off, job.n)
	var read int64
	for {
		n, err := r.Read(buf)
		crc = crc32.Update(crc, table, buf[:n])
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
	*job.digest = crc

	return nil
}
