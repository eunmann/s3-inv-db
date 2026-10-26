package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestStaticAsset_Caching: the URL pages link (?v=<this build's content
// hash>) is cached for good; a bare URL, or one naming another build's
// hash, revalidates every time, so a browser never keeps this build's
// content under another version's URL.
func TestStaticAsset_Caching(t *testing.T) {
	h := staticAsset("text/plain", []byte("x"), "abc")
	cases := []struct{ url, want string }{
		{"/static/live.js?v=abc", "public, max-age=31536000, immutable"},
		{"/static/live.js?v=old", "no-cache"},
		{"/static/live.js", "no-cache"},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, c.url, http.NoBody))
		if got := w.Header().Get("Cache-Control"); got != c.want {
			t.Errorf("%s: Cache-Control = %q, want %q", c.url, got, c.want)
		}
		if got := w.Header().Get("ETag"); got != `W/"abc"` {
			t.Errorf("%s: ETag = %q, want %q", c.url, got, `W/"abc"`)
		}
	}
}

// TestStaticAsset_NotModified: a revalidation that names this build's
// tag gets a bodiless 304 that still carries the ETag and the
// Cache-Control of its URL, so the browser's copy keeps its lifetime.
func TestStaticAsset_NotModified(t *testing.T) {
	h := staticAsset("text/plain", []byte("x"), "abc")
	cases := []struct{ url, cacheControl string }{
		{"/static/live.js?v=abc", "public, max-age=31536000, immutable"},
		{"/static/live.js", "no-cache"},
	}
	for _, c := range cases {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, c.url, http.NoBody)
		req.Header.Set("If-None-Match", `W/"abc"`)
		w := httptest.NewRecorder()
		h(w, req)
		if w.Code != http.StatusNotModified {
			t.Fatalf("%s: status %d, want 304", c.url, w.Code)
		}
		if w.Body.Len() != 0 {
			t.Errorf("%s: 304 has a %d-byte body", c.url, w.Body.Len())
		}
		if got := w.Header().Get("ETag"); got != `W/"abc"` {
			t.Errorf("%s: 304 ETag = %q, want %q", c.url, got, `W/"abc"`)
		}
		if got := w.Header().Get("Cache-Control"); got != c.cacheControl {
			t.Errorf("%s: 304 Cache-Control = %q, want %q", c.url, got, c.cacheControl)
		}
	}
}

func TestNoneMatch(t *testing.T) {
	cases := []struct {
		name   string
		fields []string
		want   bool
	}{
		{"absent", nil, false},
		{"weak", []string{`W/"abc"`}, true},
		{"strong form compares weakly", []string{`"abc"`}, true},
		{"other tag", []string{`W/"old"`}, false},
		{"list", []string{`W/"old", W/"abc"`}, true},
		{"list without spaces", []string{`"old","abc"`}, true},
		{"list of others", []string{`W/"old", "older"`}, false},
		{"repeated field", []string{`W/"old"`, `W/"abc"`}, true},
		{"star", []string{"*"}, true},
		{"prefix of tag", []string{`W/"ab"`}, false},
		{"unquoted", []string{`abc`}, false},
		{"unterminated", []string{`W/"abc`}, false},
		{"empty", []string{""}, false},
	}
	for _, c := range cases {
		if got := noneMatch(c.fields, "abc"); got != c.want {
			t.Errorf("%s: noneMatch(%q) = %v, want %v", c.name, c.fields, got, c.want)
		}
	}
}
