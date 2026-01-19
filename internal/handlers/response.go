// Package handlers provides HTTP handlers for the S3 inventory server.
package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
)

// APIResponse is the standard JSON response wrapper.
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// WantsJSON checks if the request wants JSON response.
// Returns true if Accept header includes application/json or format=json query param.
func WantsJSON(r *http.Request) bool {
	// Check format query parameter first
	if r.URL.Query().Get("format") == "json" {
		return true
	}

	// Check Accept header
	accept := r.Header.Get("Accept")
	return strings.Contains(accept, "application/json")
}

// WriteJSON writes a successful JSON response.
func WriteJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	resp := APIResponse{
		Success: status >= 200 && status < 300,
		Data:    data,
	}

	//nolint:errchkjson // Response writer errors are handled by the HTTP server
	_ = json.NewEncoder(w).Encode(resp)
}

// WriteJSONError writes an error JSON response.
func WriteJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	resp := APIResponse{
		Success: false,
		Error:   message,
	}

	//nolint:errchkjson // Response writer errors are handled by the HTTP server
	_ = json.NewEncoder(w).Encode(resp)
}

// WriteJSONDirect writes raw data as JSON without the APIResponse wrapper.
func WriteJSONDirect(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	//nolint:errchkjson // Response writer errors are handled by the HTTP server
	_ = json.NewEncoder(w).Encode(data)
}
