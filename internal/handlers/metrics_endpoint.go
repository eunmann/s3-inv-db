package handlers

import (
	"net/http"

	"github.com/rs/zerolog"
)

// MetricsHandler serves the Prometheus text exposition format from the
// handler-scoped registry.
func (h *Handlers) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if _, err := h.reg.WriteTo(w); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("write metrics")
	}
}
