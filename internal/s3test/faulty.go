//go:build integration

package s3test

import (
	"io"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

// FaultyFetchClient is FetchClient whose GET bodies go through fault,
// which may return a body that fails, ends early or changes bytes. It
// sits above the transport, so the SDK sees what fault returns.
func FaultyFetchClient(tb testing.TB, fault func(req *http.Request, body io.ReadCloser) io.ReadCloser) *s3fetch.Client {
	tb.Helper()
	mc := loadLocalConfig(tb)
	base := mc.cfg.HTTPClient
	if base == nil {
		base = awshttp.NewBuildableClient()
	}
	mc.cfg.HTTPClient = faultyHTTPClient{base: base, fault: fault}

	return s3fetch.NewClientWithConfig(mc.cfg)
}

type faultyHTTPClient struct {
	base  httpDoer
	fault func(req *http.Request, body io.ReadCloser) io.ReadCloser
}

func (c faultyHTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.base.Do(req)
	if err != nil {
		return nil, err //nolint:wrapcheck // a transport passes the inner client's result through
	}
	if req.Method == http.MethodGet && resp.StatusCode < http.StatusMultipleChoices {
		resp.Body = c.fault(req, resp.Body)
	}

	return resp, nil
}

// FailAfter is a body that gives n bytes of body, then err.
func FailAfter(body io.ReadCloser, n int64, err error) io.ReadCloser {
	return &failAfter{body: body, left: n, err: err}
}

type failAfter struct {
	body io.ReadCloser
	err  error
	left int64
}

func (f *failAfter) Read(p []byte) (int, error) {
	if f.left <= 0 {
		return 0, f.err
	}
	if int64(len(p)) > f.left {
		p = p[:f.left]
	}
	n, err := f.body.Read(p)
	f.left -= int64(n)

	return n, err //nolint:wrapcheck // io.Reader contract: io.EOF must pass through unwrapped
}

func (f *failAfter) Close() error {
	return f.body.Close() //nolint:wrapcheck // passes the transport's close error through
}
