// Package handlers provides HTTP handlers for the S3 inventory server.
package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/rs/zerolog/log"
)

// errorBody is the JSON shape returned for non-2xx API responses. Success
// responses serialize the payload directly — the HTTP status already
// conveys success, so an envelope `{success, data, error}` adds nothing.
type errorBody struct {
	Error string `json:"error"`
}

// WriteJSON writes data as a JSON response with the given status code.
// The response headers have already been committed by the time the
// encoder runs, so a write failure (typically a dropped client
// connection) can't be propagated — it's logged at debug level so it
// doesn't fill server logs but stays diagnosable.
func WriteJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Debug().Err(err).Int("status", status).Msg("write JSON response")
	}
}

// WriteJSONError writes a JSON error response of the shape `{"error":"…"}`.
func WriteJSONError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, errorBody{Error: message})
}
