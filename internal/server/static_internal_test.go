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
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/static/live.js", http.NoBody)
	req.Header.Set("If-None-Match", `W/"abc"`)
	w := httptest.NewRecorder()
	h(w, req)
	if w.Code != http.StatusNotModified {
		t.Errorf("matching If-None-Match: status %d, want 304", w.Code)
	}
}
