package extsort

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/klauspost/compress/s2"
)

var runCodecs = []RunCodec{RunCodecZstd, RunCodecS2}

// writeCodecRun writes rows to a run at path with codec.
func writeCodecRun(t *testing.T, path string, codec RunCodec, level CompressionLevel, rows []PrefixRow) {
	t.Helper()
	w, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{Codec: codec, CompressionLevel: level})
	if err != nil {
		t.Fatalf("create %s writer: %v", codec, err)
	}
	for i := range rows {
		if err := w.Write(&rows[i]); err != nil {
			t.Fatalf("write row %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close %s writer: %v", codec, err)
	}
}

// readCodecRun reads every row of the run at path, returning the rows
// read before the first error and that error (nil at a clean end).
func readCodecRun(t *testing.T, path string) ([]PrefixRow, error) {
	t.Helper()
	r, err := OpenCompressedRunFile(path, mergeReaderBufferSize)
	if err != nil {
		t.Fatalf("open run: %v", err)
	}
	defer r.Close()
	var rows []PrefixRow
	for {
		var row PrefixRow
		err := r.ReadInto(&row)
		if errors.Is(err, io.EOF) {
			return rows, nil
		}
		if err != nil {
			return rows, err
		}
		rows = append(rows, row)
	}
}

// runHeaderCodec returns the compression type in the run's header.
func runHeaderCodec(t *testing.T, path string) RunCodec {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return RunCodec((binary.LittleEndian.Uint32(b[8:12]) & flagCompressionMask) >> 1)
}

// hexSortedRows returns n sorted rows whose prefixes share little, so
// a run of them spans several codec blocks.
func hexSortedRows(n int) []PrefixRow {
	rng := rand.New(rand.NewPCG(1, 2))
	rows := make([]PrefixRow, n)
	for i := range rows {
		rows[i].Prefix = fmt.Sprintf("%016x/", rng.Uint64())
		rows[i].Depth = 1
		rows[i].Count = rng.Uint64N(1000) + 1
		rows[i].TotalBytes = rng.Uint64N(1 << 40)
		rows[i].TierCounts[0], rows[i].TierBytes[0] = rows[i].Count, rows[i].TotalBytes
	}
	slices.SortFunc(rows, func(a, b PrefixRow) int {
		if a.Prefix < b.Prefix {
			return -1
		}
		if a.Prefix > b.Prefix {
			return 1
		}

		return 0
	})

	return rows
}

func TestRunCodecRoundTrip(t *testing.T) {
	sets := map[string][]PrefixRow{
		"empty":  nil,
		"random": randomSortedRows(rand.New(rand.NewPCG(3, 4)), 500),
		"blocks": hexSortedRows(150_000),
	}
	for _, codec := range runCodecs {
		for _, level := range []CompressionLevel{CompressionFastest, CompressionDefault, CompressionBetter} {
			for name, rows := range sets {
				t.Run(fmt.Sprintf("%s/level=%d/%s", codec, level, name), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "run.crun")
					writeCodecRun(t, path, codec, level, rows)
					if got := runHeaderCodec(t, path); got != codec {
						t.Fatalf("header codec = %s, want %s", got, codec)
					}
					got, err := readCodecRun(t, path)
					if err != nil {
						t.Fatalf("read: %v", err)
					}
					if !slices.Equal(got, rows) {
						t.Fatalf("read %d rows back unequal to the %d written", len(got), len(rows))
					}
				})
			}
		}
	}
}

func TestRunCodecDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.crun")
	writeCodecRun(t, path, 0, 0, randomSortedRows(rand.New(rand.NewPCG(1, 1)), 10))
	if got := runHeaderCodec(t, path); got != defaultRunCodec {
		t.Fatalf("header codec = %s, want the default %s", got, defaultRunCodec)
	}
}

// TestRunCodecMixedMerge merges runs of both codecs through a disk
// round written in each codec: the reader takes every run's codec from
// its header, and the sums must equal a direct merge of the rows.
func TestRunCodecMixedMerge(t *testing.T) {
	for _, outCodec := range runCodecs {
		t.Run("rounds="+outCodec.String(), func(t *testing.T) {
			dir := t.TempDir()
			want := map[string]PrefixRow{}
			const numRuns = 8
			paths := make([]string, 0, numRuns)
			for i := range numRuns {
				rows := randomSortedRows(rand.New(rand.NewPCG(uint64(i), 9)), 200)
				for j := range rows {
					acc, ok := want[rows[j].Prefix]
					if !ok {
						want[rows[j].Prefix] = rows[j]

						continue
					}
					acc.Merge(&rows[j])
					want[rows[j].Prefix] = acc
				}
				path := filepath.Join(dir, fmt.Sprintf("run%d.crun", i))
				writeCodecRun(t, path, runCodecs[i%len(runCodecs)], CompressionFastest, rows)
				paths = append(paths, path)
			}

			merger := NewParallelMerger(ParallelMergeConfig{
				NumWorkers: 2,
				MaxFanIn:   2,
				FinalFanIn: 2,
				TempDir:    dir,
				Codec:      outCodec,
			})
			got := mergeAllRows(t, merger, paths)
			if rounds := merger.Statistics().Rounds; rounds < 1 {
				t.Fatalf("rounds = %d, want a disk round", rounds)
			}

			if len(got) != len(want) {
				t.Fatalf("merged %d rows, want %d", len(got), len(want))
			}
			for i := range got {
				if w := want[got[i].Prefix]; got[i] != w {
					t.Fatalf("row %q = %+v, want %+v", got[i].Prefix, got[i], w)
				}
			}
			if !slices.IsSorted(rowPrefixes(got)) {
				t.Fatal("merged rows are not sorted")
			}
		})
	}
}

func TestRunCodecHeaderValidation(t *testing.T) {
	t.Run("writer rejects an unknown codec", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "run.crun")
		_, err := NewCompressedRunWriter(path, CompressedRunWriterOptions{Codec: 3})
		if !errors.Is(err, ErrUnsupportedCompression) {
			t.Fatalf("err = %v, want ErrUnsupportedCompression", err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("writer left a file behind (stat err %v)", err)
		}
	})

	for _, typ := range []uint32{0, 3, 7} {
		t.Run(fmt.Sprintf("reader rejects type %d", typ), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "run.crun")
			writeCodecRun(t, path, RunCodecS2, CompressionFastest, randomSortedRows(rand.New(rand.NewPCG(1, 2)), 5))
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt(binary.LittleEndian.AppendUint32(nil, flagCompressed|typ<<1), 8); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := OpenCompressedRunFile(path, 0)
			if !errors.Is(err, ErrUnsupportedCompression) {
				if r != nil {
					r.Close()
				}
				t.Fatalf("err = %v, want ErrUnsupportedCompression", err)
			}
		})
	}
}

// TestRunCodecS2Corrupt: a damaged S2 body fails the read; a merge
// that stopped quietly would drop rows.
func TestRunCodecS2Corrupt(t *testing.T) {
	rows := hexSortedRows(150_000)
	damage := map[string]func(t *testing.T, path string, size int64){
		"truncated mid-block": func(t *testing.T, path string, size int64) {
			t.Helper()
			if err := os.Truncate(path, compressedRunFileHeader+(size-compressedRunFileHeader)/2); err != nil {
				t.Fatal(err)
			}
		},
		"flipped body byte": func(t *testing.T, path string, size int64) {
			t.Helper()
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			at := compressedRunFileHeader + (size-compressedRunFileHeader)/3
			b := make([]byte, 1)
			if _, err := f.ReadAt(b, at); err != nil {
				t.Fatal(err)
			}
			b[0] ^= 0x5a
			if _, err := f.WriteAt(b, at); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, apply := range damage {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "run.crun")
			writeCodecRun(t, path, RunCodecS2, CompressionFastest, rows)
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			apply(t, path, info.Size())

			got, err := readCodecRun(t, path)
			if err == nil {
				t.Fatalf("read %d of %d rows without error", len(got), len(rows))
			}
			if !errors.Is(err, errRunRecordCorrupt) && !errors.Is(err, s2.ErrCorrupt) && !errors.Is(err, s2.ErrCRC) {
				t.Fatalf("err = %v, want errRunRecordCorrupt or an S2 decode error", err)
			}
		})
	}
}

// TestS2RunReaderHeap keeps openRunHeapBytes honest for S2: an open
// reader past its first full block holds no more than the merge budgets
// per open run.
func TestS2RunReaderHeap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.crun")
	writeCodecRun(t, path, RunCodecS2, CompressionFastest, hexSortedRows(150_000))

	const n = 16
	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	readers := make([]*CompressedRunReader, n)
	for i := range readers {
		r, err := OpenCompressedRunFile(path, mergeReaderBufferSize)
		if err != nil {
			t.Fatal(err)
		}
		var row PrefixRow
		if err := r.ReadInto(&row); err != nil {
			t.Fatal(err)
		}
		readers[i] = r
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	perReader := (int64(after.HeapAlloc) - int64(before.HeapAlloc)) / n
	for _, r := range readers {
		r.Close()
	}

	t.Logf("S2 open run reader heap: %d bytes (budget %d)", perReader, openRunHeapBytes)
	if perReader > openRunHeapBytes {
		t.Fatalf("S2 open run reader holds %d bytes, over openRunHeapBytes %d", perReader, openRunHeapBytes)
	}
}
