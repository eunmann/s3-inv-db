package templates_test

import (
	"bytes"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"html"
	"regexp"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/templates"
)

// htmxSRI is the published Subresource Integrity hash of htmx 1.9.10's
// dist/htmx.min.js.
const htmxSRI = "sha384-D1Kt99CQMDuVetoL1lrYwg5t+9QdHe7NLX/SoJYkXDFfX37iInKRy5xLSi8nO7UC"

// TestHtmxJS_IsPublishedRelease: the vendored htmx is the published
// 1.9.10 file, unmodified.
func TestHtmxJS_IsPublishedRelease(t *testing.T) {
	sum := sha512.Sum384(templates.HtmxJS())
	if got := "sha384-" + base64.StdEncoding.EncodeToString(sum[:]); got != htmxSRI {
		t.Errorf("scripts/htmx.min.js integrity = %s, want %s", got, htmxSRI)
	}
}

// TestLayout_LoadsVendoredHtmx: every page loads htmx from this server
// under its versioned URL, before live.js, with the hardened config.
func TestLayout_LoadsVendoredHtmx(t *testing.T) {
	renderer, err := templates.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var buf bytes.Buffer
	if err := renderer.Render(&buf, "dashboard.html", map[string]any{"Title": "Dashboard"}); err != nil {
		t.Fatalf("Render: %v", err)
	}
	page := buf.String()

	if strings.Contains(page, "unpkg.com") {
		t.Error("page still loads a script from unpkg.com")
	}
	htmxTag := `<script src="/static/htmx.min.js?v=` + templates.AssetVersion("htmx.min.js") + `"></script>`
	htmxAt := strings.Index(page, htmxTag)
	if htmxAt < 0 {
		t.Fatalf("page missing %s", htmxTag)
	}
	if liveAt := strings.Index(page, `/static/live.js?v=`); liveAt < htmxAt {
		t.Errorf("live.js (at %d) must load after htmx (at %d)", liveAt, htmxAt)
	}

	m := regexp.MustCompile(`<meta name="htmx-config" content='([^']*)'>`).FindStringSubmatch(page)
	if m == nil {
		t.Fatal("page missing the htmx-config meta tag")
	}
	var cfg map[string]any
	if err := json.Unmarshal([]byte(html.UnescapeString(m[1])), &cfg); err != nil {
		t.Fatalf("htmx-config is not JSON: %v (%s)", err, m[1])
	}
	want := map[string]any{"selfRequestsOnly": true, "allowEval": false, "historyCacheSize": float64(0)}
	for k, v := range want {
		if cfg[k] != v {
			t.Errorf("htmx-config %s = %v, want %v", k, cfg[k], v)
		}
	}
	if len(cfg) != len(want) {
		t.Errorf("htmx-config = %v, want exactly %v", cfg, want)
	}
}
