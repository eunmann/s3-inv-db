package handlers

import (
	"net/http"
)

// HelpPage renders the user-facing help / quick-start page. Content is
// entirely static — the template explains the UI and workflows the
// other pages drive.
func (h *Handlers) HelpPage(w http.ResponseWriter, r *http.Request) {
	data := map[string]any{
		"Title":        "Help",
		"S3Source":     h.s3SourceURI,
		"HasDiscovery": h.discovery.Enabled(),
	}
	h.renderHTML(w, r, "help.html", "render help page", data)
}
