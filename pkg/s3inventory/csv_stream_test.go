package s3inventory_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/eunmann/s3-inv-db/pkg/s3inventory"
)

// gzipMembers gzips each member and concatenates them: a multi-member
// gzip file when given more than one.
func gzipMembers(tb testing.TB, members ...string) []byte {
	tb.Helper()
	var buf bytes.Buffer
	for _, m := range members {
		w := gzip.NewWriter(&buf)
		if _, err := w.Write([]byte(m)); err != nil {
			tb.Fatal(err)
		}
		if err := w.Close(); err != nil {
			tb.Fatal(err)
		}
	}

	return buf.Bytes()
}

// closeCounter is a stream that counts its Close calls.
type closeCounter struct {
	io.Reader

	closes int
}

func (c *closeCounter) Close() error {
	c.closes++

	return nil
}

var streamCfg = s3inventory.CSVReaderConfig{KeyCol: 0, SizeCol: 1, StorageCol: 2, AccessTierCol: -1, DeleteMarkerCol: -1}

// readStream reads data through NewCSVReaderFromStream until EOF or an
// error, returning the rows as "key:size", and checks that Close closes
// the stream once and, after a clean read, succeeds.
func readStream(t *testing.T, data []byte) ([]string, s3inventory.Anomalies, error) {
	t.Helper()
	stream := &closeCounter{Reader: bytes.NewReader(data)}
	r, err := s3inventory.NewCSVReaderFromStream(stream, streamCfg)
	if err != nil {
		t.Fatalf("NewCSVReaderFromStream: %v", err)
	}
	var rows []string
	var readErr error
	for {
		row, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			readErr = err

			break
		}
		rows = append(rows, fmt.Sprintf("%s:%d", row.Key, row.Size))
	}
	anomalies := r.Anomalies()
	if err := r.Close(); err != nil && readErr == nil {
		t.Errorf("Close after a clean read: %v", err)
	}
	if stream.closes != 1 {
		t.Errorf("stream closed %d times, want 1", stream.closes)
	}

	return rows, anomalies, readErr
}

func TestCSVReaderFromStreamDetectsGzipByContent(t *testing.T) {
	const csv = "a/x.txt,100,STANDARD\nb/y.txt,23,GLACIER\n"
	want := []string{"a/x.txt:100", "b/y.txt:23"}
	for _, c := range []struct {
		name      string
		data      []byte
		want      []string
		shortRows uint64
	}{
		{"plain", []byte(csv), want, 0},
		{"gzip", gzipMembers(t, csv), want, 0},
		{"multi-member gzip", gzipMembers(t, "a/x.txt,100,STANDARD\n", "b/y.txt,23,GLACIER\n"), want, 0},
		{"gzip member ends mid-line", gzipMembers(t, "a/x.txt,10", "0,STANDARD\nb/y.txt,23,GLACIER\n"), want, 0},
		{"gzip of nothing", gzipMembers(t, ""), nil, 0},
		{"empty", nil, nil, 0},
		{"first magic byte only", []byte{0x1f}, nil, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			rows, anomalies, err := readStream(t, c.data)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if !slices.Equal(rows, c.want) {
				t.Errorf("rows = %q, want %q", rows, c.want)
			}
			if anomalies.ShortRows != c.shortRows {
				t.Errorf("ShortRows = %d, want %d", anomalies.ShortRows, c.shortRows)
			}
		})
	}
}

// A gzip stream that is cut short, fails its checksum or carries bytes
// that are not another member fails the read: a chunk is never
// indexed with rows missing.
func TestCSVReaderFromStreamCorruptGzipFails(t *testing.T) {
	good := gzipMembers(t, strings.Repeat("a/x.txt,100,STANDARD\n", 1000))
	badCRC := slices.Clone(good)
	badCRC[len(badCRC)-8] ^= 0xff
	for _, c := range []struct {
		name string
		data []byte
	}{
		{"truncated", good[:len(good)/2]},
		{"truncated trailer", good[:len(good)-3]},
		{"bad checksum", badCRC},
		{"trailing garbage", append(slices.Clone(good), "not gzip"...)},
	} {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := readStream(t, c.data); err == nil {
				t.Error("read succeeded, want an error")
			}
		})
	}
}

var errRead = errors.New("connection reset")

func TestCSVReaderFromStreamReadErrorClosesStream(t *testing.T) {
	stream := &closeCounter{Reader: iotest.ErrReader(errRead)}
	if _, err := s3inventory.NewCSVReaderFromStream(stream, streamCfg); !errors.Is(err, errRead) {
		t.Fatalf("NewCSVReaderFromStream error = %v, want %v", err, errRead)
	}
	if stream.closes != 1 {
		t.Errorf("stream closed %d times, want 1", stream.closes)
	}
}

var (
	errTruncatedRead = errors.New("truncated stream read without error")
	errRowsDiffer    = errors.New("rows differ from those written")
)

// pooledStream is a stream for TestCSVReaderFromStreamReuseKeepsStreamsApart.
type pooledStream struct {
	data []byte
	want []string
	bad  bool
}

// checkPooledStream reads s and reports how it differs from what was
// written: rows, or for a bad stream, an error.
func checkPooledStream(s pooledStream) error {
	r, err := s3inventory.NewCSVReaderFromStream(io.NopCloser(bytes.NewReader(s.data)), streamCfg)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer r.Close()
	var rows []string
	for {
		row, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if s.bad {
				return nil
			}

			return fmt.Errorf("next: %w", err)
		}
		rows = append(rows, fmt.Sprintf("%s:%d", row.Key, row.Size))
	}
	if s.bad {
		return errTruncatedRead
	}
	if !slices.Equal(rows, s.want) {
		return fmt.Errorf("%w: read %d, wrote %d", errRowsDiffer, len(rows), len(s.want))
	}

	return nil
}

// Readers opened one after another and side by side reuse pooled
// buffers and gunzip state; each still reads exactly its own stream,
// whatever the reader before it read or how it failed.
func TestCSVReaderFromStreamReuseKeepsStreamsApart(t *testing.T) {
	streams := make([]pooledStream, 0, 9)
	for i := range 8 {
		var csv strings.Builder
		var want []string
		for j := range 500 + i*300 {
			fmt.Fprintf(&csv, "s%d/k%d,%d,STANDARD\n", i, j, j)
			want = append(want, fmt.Sprintf("s%d/k%d:%d", i, j, j))
		}
		data := []byte(csv.String())
		if i%2 == 0 {
			data = gzipMembers(t, csv.String())
		}
		streams = append(streams, pooledStream{data: data, want: want})
	}
	streams = append(streams, pooledStream{data: streams[0].data[:len(streams[0].data)/2], bad: true})

	for range 3 {
		for _, s := range streams {
			if err := checkPooledStream(s); err != nil {
				t.Fatal(err)
			}
		}
	}

	const workers = 8
	errs := make(chan error, workers)
	for w := range workers {
		go func() {
			for k := range 4 * len(streams) {
				if err := checkPooledStream(streams[(w+k)%len(streams)]); err != nil {
					errs <- err

					return
				}
			}
			errs <- nil
		}()
	}
	for range workers {
		if err := <-errs; err != nil {
			t.Error(err)
		}
	}
}

// Gunzip runs on the reader's goroutine: the build already reads one
// chunk per CPU, so decompressor goroutines would only add handoffs.
func TestCSVReaderFromStreamGunzipsInline(t *testing.T) {
	data := gzipMembers(t, strings.Repeat("a/x.txt,100,STANDARD\n", 100_000))
	before := runtime.NumGoroutine()
	r, err := s3inventory.NewCSVReaderFromStream(io.NopCloser(bytes.NewReader(data)), streamCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("reading a gzip stream started %d goroutines", after-before)
	}
}

func TestCSVReaderCloseTwice(t *testing.T) {
	stream := &closeCounter{Reader: bytes.NewReader(gzipMembers(t, "a/x.txt,100,STANDARD\n"))}
	r, err := s3inventory.NewCSVReaderFromStream(stream, streamCfg)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := r.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if stream.closes != 1 {
		t.Errorf("stream closed %d times, want 1", stream.closes)
	}
}
