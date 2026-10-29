package s3inventory_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
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
