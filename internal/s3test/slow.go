//go:build integration

package s3test

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

// SlowFetchClient is FetchClient behind a slower link: every request
// waits latency for its first byte and every GET body streams at most
// bytesPerSec. The local server answers in microseconds, so without it
// a benchmark never sees the time a build spends waiting on S3.
func SlowFetchClient(tb testing.TB, latency time.Duration, bytesPerSec int64) *s3fetch.Client {
	tb.Helper()
	mc := loadLocalConfig(tb)
	base := mc.cfg.HTTPClient
	if base == nil {
		base = awshttp.NewBuildableClient()
	}
	mc.cfg.HTTPClient = slowHTTPClient{base: base, latency: latency, bytesPerSec: bytesPerSec}

	return s3fetch.NewClientWithConfig(mc.cfg)
}

type httpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type slowHTTPClient struct {
	base        httpDoer
	latency     time.Duration
	bytesPerSec int64
}

func (c slowHTTPClient) Do(req *http.Request) (*http.Response, error) {
	select {
	case <-time.After(c.latency):
	case <-req.Context().Done():
		return nil, fmt.Errorf("slow link: %w", req.Context().Err())
	}
	resp, err := c.base.Do(req)
	if err != nil {
		return nil, err //nolint:wrapcheck // a transport passes the inner client's result through
	}
	if req.Method == http.MethodGet {
		resp.Body = &slowBody{body: resp.Body, bytesPerSec: c.bytesPerSec, start: time.Now()}
	}

	return resp, nil
}

// slowBody paces reads so the body never runs ahead of bytesPerSec.
type slowBody struct {
	body        io.ReadCloser
	start       time.Time
	bytesPerSec int64
	read        int64
}

func (b *slowBody) Read(p []byte) (int, error) {
	const maxRead = 64 << 10
	if len(p) > maxRead {
		p = p[:maxRead]
	}
	n, err := b.body.Read(p)
	b.read += int64(n)
	due := time.Duration(float64(b.read) / float64(b.bytesPerSec) * float64(time.Second))
	if wait := due - time.Since(b.start); wait > 0 {
		time.Sleep(wait)
	}

	return n, err //nolint:wrapcheck // io.Reader contract: io.EOF must pass through unwrapped
}

func (b *slowBody) Close() error {
	return b.body.Close() //nolint:wrapcheck // passes the transport's close error through
}
