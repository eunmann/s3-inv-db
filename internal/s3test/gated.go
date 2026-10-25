//go:build integration

package s3test

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
)

// Gate holds the GET bodies of a GatedFetchClient until it is opened.
type Gate struct {
	stalled     chan struct{}
	release     chan struct{}
	stalledOnce sync.Once
	openOnce    sync.Once
}

// Stalled is closed once the first body has stalled at the gate.
func (g *Gate) Stalled() <-chan struct{} { return g.stalled }

// Open lets every held body, and every later one, read through.
func (g *Gate) Open() { g.openOnce.Do(func() { close(g.release) }) }

// GatedFetchClient is FetchClient whose GET bodies for paths containing
// match stall on their first read until the gate opens, even when the
// request is cancelled: a stand-in for a transfer that is slow to
// unwind after a cancel. The gate opens at test cleanup if not before.
func GatedFetchClient(tb testing.TB, match string) (*s3fetch.Client, *Gate) {
	tb.Helper()
	gate := &Gate{stalled: make(chan struct{}), release: make(chan struct{})}
	tb.Cleanup(gate.Open)
	mc := loadLocalConfig(tb)
	base := mc.cfg.HTTPClient
	if base == nil {
		base = awshttp.NewBuildableClient()
	}
	mc.cfg.HTTPClient = gatedHTTPClient{base: base, match: match, gate: gate}

	return s3fetch.NewClientWithConfig(mc.cfg), gate
}

type gatedHTTPClient struct {
	base  httpDoer
	gate  *Gate
	match string
}

func (c gatedHTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.base.Do(req)
	if err != nil {
		return nil, err //nolint:wrapcheck // a transport passes the inner client's result through
	}
	if req.Method == http.MethodGet && strings.Contains(req.URL.Path, c.match) {
		resp.Body = &gatedBody{body: resp.Body, gate: c.gate}
	}

	return resp, nil
}

type gatedBody struct {
	body io.ReadCloser
	gate *Gate
}

func (b *gatedBody) Read(p []byte) (int, error) {
	b.gate.stalledOnce.Do(func() { close(b.gate.stalled) })
	<-b.gate.release

	return b.body.Read(p) //nolint:wrapcheck // io.Reader contract: io.EOF must pass through unwrapped
}

func (b *gatedBody) Close() error {
	return b.body.Close() //nolint:wrapcheck // passes the transport's close error through
}
