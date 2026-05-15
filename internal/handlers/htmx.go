package handlers

import "net/http"

// wantsHTMXPartial reports whether the request expects an htmx-partial
// response rather than a full HTML page. A boosted nav click and a
// history-restore both set HX-Request, but the user is navigating
// pages — they need the full layout.
func wantsHTMXPartial(r *http.Request) bool {
	return r.Header.Get("HX-Request") == trueLiteral &&
		r.Header.Get("HX-Boosted") != trueLiteral &&
		r.Header.Get("HX-History-Restore-Request") != trueLiteral
}
