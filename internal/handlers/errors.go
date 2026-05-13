package handlers

import (
	"errors"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/logctx"
)

// errPrefixNotFound is returned by buildStatsResponse and other index
// query helpers when a requested prefix isn't in the index. It maps to
// 404 like a regular not-found.
var errPrefixNotFound = errors.New("prefix not found")

// managerErrorStatus maps a Manager / index-query error to the HTTP
// status code and client-visible message that handlers should return.
// Unknown errors collapse to 500 + a generic message so internal error
// strings (S3 SDK codes, file paths) don't leak to clients.
//
// JSON handlers wrap the result via WriteJSONError; partial (HTML)
// handlers use http.Error. Centralising the mapping keeps the JSON and
// HTML twins in lock-step: fixing a status code here updates both.
func managerErrorStatus(err error) (status int, msg string) {
	switch {
	case errors.Is(err, inventory.ErrNotFound),
		errors.Is(err, errPrefixNotFound):
		return http.StatusNotFound, errMessage(err)
	case errors.Is(err, inventory.ErrNotLoaded):
		return http.StatusConflict, "inventory not loaded"
	case errors.Is(err, inventory.ErrInvalidState):
		return http.StatusConflict, err.Error()
	}
	return http.StatusInternalServerError, "operation failed"
}

func errMessage(err error) string {
	switch {
	case errors.Is(err, inventory.ErrNotFound):
		return "inventory not found"
	case errors.Is(err, errPrefixNotFound):
		return "prefix not found"
	}
	return err.Error()
}

// respondManagerErrorHTML emits a text/plain (http.Error) response for
// the same set of errors. Used by /partials/* handlers because htmx
// surfaces non-2xx response bodies verbatim. (The JSON counterpart
// is unused now that the /api/discovered mutating routes are gone;
// re-introduce it as `respondManagerError` if a JSON mutator returns.)
func respondManagerErrorHTML(w http.ResponseWriter, r *http.Request, err error, op string) {
	status, msg := managerErrorStatus(err)
	if status >= http.StatusInternalServerError {
		logctx.FromContext(r.Context()).Error().Err(err).Str("op", op).Msg("manager error")
	}
	http.Error(w, msg, status)
}
