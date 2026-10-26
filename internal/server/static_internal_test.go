package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestStaticAsset_Caching: a content-versioned URL (?v=, as pages link
// it) is cached for good; a bare URL revalidates every time, so a deploy
// never leaves browsers running an old script against new pages.
func TestStaticAsset_Caching(t *testing.T) {
	h := staticAsset("text/plain", func() []byte { return []byte("x") }, func() string { return `W/"abc"` })
	cases := []struct{ url, want string }{
		{"/static/live.js?v=abc", "public, max-age=31536000, immutable"},
		{"/static/live.js", "no-cache"},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, c.url, http.NoBody))
		if got := w.Header().Get("Cache-Control"); got != c.want {
			t.Errorf("%s: Cache-Control = %q, want %q", c.url, got, c.want)
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
