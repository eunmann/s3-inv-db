// Package handlers provides HTTP handlers for the S3 inventory server.
package handlers

import (
	"encoding/json"
	"net/http"
)

// errorBody is the JSON shape returned for non-2xx API responses. Success
// responses serialize the payload directly — the HTTP status already
// conveys success, so an envelope `{success, data, error}` adds nothing.
type errorBody struct {
	Error string `json:"error"`
}

// WriteJSON writes data as a JSON response with the given status code.
func WriteJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	//nolint:errchkjson // a closed/dropped connection is handled by the HTTP server
	_ = json.NewEncoder(w).Encode(data)
}

// WriteJSONError writes a JSON error response of the shape `{"error":"…"}`.
func WriteJSONError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, errorBody{Error: message})
}
