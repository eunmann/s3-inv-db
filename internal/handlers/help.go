package handlers

import (
	"net/http"

	"github.com/rs/zerolog"
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
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.Render(w, "help.html", data); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("render help page")
		http.Error(w, "failed to render page", http.StatusInternalServerError)
	}
}
