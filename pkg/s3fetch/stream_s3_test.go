//go:build integration

package s3fetch_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/eunmann/s3-inv-db/internal/s3test"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

var errDropped = errors.New("connection dropped")

// putMultipart uploads body as a multipart object of 5 MiB parts.
func putMultipart(t *testing.T, c *s3.Client, bucket, key string, body []byte) {
	t.Helper()
	ctx := t.Context()
	mp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	const partSize = 5 << 20
	var parts []types.CompletedPart
	for i := 0; i*partSize < len(body); i++ {
		num := aws.Int32(int32(i + 1))
		out, err := c.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: aws.String(bucket), Key: aws.String(key), UploadId: mp.UploadId, PartNumber: num,
			Body: bytes.NewReader(body[i*partSize : min((i+1)*partSize, len(body))]),
		})
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, types.CompletedPart{ETag: out.ETag, PartNumber: num})
	}
	if _, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: mp.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		t.Fatal(err)
	}
}

func streamBytes(t *testing.T, c *s3fetch.Client, bucket, key string) ([]byte, error) {
	t.Helper()
	s, err := c.OpenStream(t.Context(), bucket, key)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer s.Close()
	got, err := io.ReadAll(s)
	if err == nil && int64(len(got)) != s.Size() {
		t.Fatalf("read %d bytes, Size says %d", len(got), s.Size())
	}

	return got, err //nolint:wrapcheck // the test checks the stream's own error
}

// patterned is n bytes that differ from part to part and within one.
func patterned(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/4093)
	}

	return b
}

func TestOpenStream_RoundTrip(t *testing.T) {
	c := s3test.FetchClient(t)
	bucket := s3test.Bucket(t, c.Raw())
	single := patterned(3<<20 + 17)
	multi := patterned(11<<20 + 1234)
	putObject(t, c.Raw(), bucket, "single.bin", single)
	putMultipart(t, c.Raw(), bucket, "multi.bin", multi)

	for key, want := range map[string][]byte{"single.bin": single, "multi.bin": multi} {
		got, err := streamBytes(t, c, bucket, key)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s: read %d bytes that differ from the object's %d", key, len(got), len(want))
		}
	}
}

func TestOpenStream_Missing(t *testing.T) {
	c := s3test.FetchClient(t)
	bucket := s3test.Bucket(t, c.Raw())
	if _, err := c.OpenStream(t.Context(), bucket, "absent.bin"); err == nil {
		t.Fatal("OpenStream on a missing object returned nil error")
	}
}

// flipped changes one byte of the body, as a fault no TCP or TLS check
// caught would.
type flipped struct {
	io.ReadCloser

	done bool
}

func (f *flipped) Read(p []byte) (int, error) {
	n, err := f.ReadCloser.Read(p)
	if n > 0 && !f.done {
		p[0] ^= 0xff
		f.done = true
	}

	return n, err //nolint:wrapcheck // io.Reader contract: io.EOF must pass through unwrapped
}

// The object's checksum is checked as it streams: a body changed in
// transit fails the stream, and is not requested again, since the
// changed bytes went to the reader before the checksum showed them.
func TestOpenStream_ChecksumMismatch(t *testing.T) {
	raw := s3test.RawClient(t)
	bucket := s3test.Bucket(t, raw)
	putObject(t, raw, bucket, "blob.bin", patterned(2<<20))

	var gets atomic.Int64
	c := s3test.FaultyFetchClient(t, func(_ *http.Request, body io.ReadCloser) io.ReadCloser {
		if gets.Add(1) == 1 {
			return &flipped{ReadCloser: body}
		}

		return body
	})
	if got, err := streamBytes(t, c, bucket, "blob.bin"); err == nil {
		t.Fatalf("a body changed in transit streamed %d bytes without error", len(got))
	}
	if n := gets.Load(); n != 1 {
		t.Errorf("%d GETs, want 1", n)
	}
}

// A connection dropped mid-part is requested again and read on; one
// that ends early every time fails rather than ending the stream short.
func TestOpenStream_DroppedBody(t *testing.T) {
	raw := s3test.RawClient(t)
	bucket := s3test.Bucket(t, raw)
	want := patterned(11 << 20)
	putMultipart(t, raw, bucket, "multi.bin", want)

	var mu sync.Mutex
	seen := map[string]int{}
	fault := func(always bool, err error) *s3fetch.Client {
		return s3test.FaultyFetchClient(t, func(req *http.Request, body io.ReadCloser) io.ReadCloser {
			part := req.URL.Query().Get("partNumber")
			mu.Lock()
			seen[part]++
			first := seen[part] == 1
			mu.Unlock()
			if part == "2" && (first || always) {
				return s3test.FailAfter(body, 1<<20+5, err)
			}

			return body
		})
	}

	got, err := streamBytes(t, fault(false, errDropped), bucket, "multi.bin")
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("dropped once: %d bytes (equal %v), %v; want the object", len(got), bytes.Equal(got, want), err)
	}
	clear(seen)
	got, err = streamBytes(t, fault(true, io.EOF), bucket, "multi.bin")
	if err == nil {
		t.Fatalf("a part that always ends early streamed %d bytes without error", len(got))
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("error = %v, want io.ErrUnexpectedEOF", err)
	}

	// The bytes read before the drop were changed in transit: the repeat
	// doesn't start with them, so it can't be read on from where the
	// drop left off.
	clear(seen)
	c := s3test.FaultyFetchClient(t, func(_ *http.Request, body io.ReadCloser) io.ReadCloser {
		mu.Lock()
		seen["all"]++
		first := seen["all"] == 1
		mu.Unlock()
		if first {
			return s3test.FailAfter(&flipped{ReadCloser: body}, 1<<20+5, errDropped)
		}

		return body
	})
	if got, err := streamBytes(t, c, bucket, "multi.bin"); err == nil {
		t.Fatalf("a repeat that differs from the bytes read streamed %d bytes without error", len(got))
	}
}
