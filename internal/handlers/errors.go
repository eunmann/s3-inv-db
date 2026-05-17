package handlers

import (
	"errors"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/rs/zerolog"
)

// errPrefixNotFound is returned by buildStatsResponse and other index
// query helpers when a requested prefix isn't in the index. It maps to
// 404 like a regular not-found.
var errPrefixNotFound = errors.New("prefix not found")

// ManagerErrorResponse is the HTTP status / client message pair returned
// by managerErrorStatus. Named so callers don't have to remember the
// positional convention.
type ManagerErrorResponse struct {
	Status  int
	Message string
}

// managerErrorStatus maps a Manager / index-query error to the HTTP
// status code and client-visible message that handlers should return.
// Unknown errors collapse to 500 + a generic message so internal error
// strings (S3 SDK codes, file paths) don't leak to clients.
//
// JSON handlers wrap the result via WriteJSONError; partial (HTML)
// handlers use http.Error. Centralising the mapping keeps the JSON and
// HTML twins in lock-step: fixing a status code here updates both.
func managerErrorStatus(err error) ManagerErrorResponse {
	switch {
	case errors.Is(err, inventory.ErrNotFound):
		return ManagerErrorResponse{Status: http.StatusNotFound, Message: "inventory not found"}
	case errors.Is(err, errPrefixNotFound):
		return ManagerErrorResponse{Status: http.StatusNotFound, Message: "prefix not found"}
	case errors.Is(err, inventory.ErrNotLoaded):
		return ManagerErrorResponse{Status: http.StatusConflict, Message: "inventory not loaded"}
	case errors.Is(err, inventory.ErrInvalidState):
		// The InvalidState message is ours ("cannot load from state X")
		// — useful diagnostic and contains no internal infrastructure detail.
		return ManagerErrorResponse{Status: http.StatusConflict, Message: err.Error()}
	}

	return ManagerErrorResponse{Status: http.StatusInternalServerError, Message: "operation failed"}
}

// respondManagerError emits a text/plain (http.Error) response for
// the manager-error set. Used by /partials/* handlers because htmx
// surfaces non-2xx response bodies verbatim.
func respondManagerError(w http.ResponseWriter, r *http.Request, err error, op string) {
	resp := managerErrorStatus(err)
	if resp.Status >= http.StatusInternalServerError {
		zerolog.Ctx(r.Context()).Error().Err(err).Str("op", op).Msg("manager error")
	}
	http.Error(w, resp.Message, resp.Status)
}
