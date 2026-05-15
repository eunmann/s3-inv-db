package handlers

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

// AutoLoadToggleResponse is the JSON shape returned by
// SetAutoLoadConfigAPI and SetPinAPI. Tiny because the UI swaps the
// page section rather than reading individual fields, but exposed for
// scripting.
type AutoLoadToggleResponse struct {
	OK bool `json:"ok"`
}

// SetAutoLoadConfigAPI flips the auto_load flag (and optionally the
// retention count) for one inventory configuration. Body is form-encoded:
// `auto_load=on|off` and optionally `retention=N`.
func (h *Handlers) SetAutoLoadConfigAPI(w http.ResponseWriter, r *http.Request) {
	if h.configStore == nil {
		WriteJSONError(w, http.StatusServiceUnavailable, "auto-load not configured")
		return
	}
	src := chi.URLParam(r, "src")
	name := chi.URLParam(r, "name")
	if src == "" || name == "" {
		WriteJSONError(w, http.StatusBadRequest, "source and name are required")
		return
	}
	if err := r.ParseForm(); err != nil {
		WriteJSONError(w, http.StatusBadRequest, "invalid form body")
		return
	}
	cfg, err := h.configStore.Get(src, name)
	if errors.Is(err, inventory.ErrStoreNotFound) {
		cfg = inventory.Config{Source: src, Name: name, RetentionCount: inventory.DefaultRetentionCount}
	} else if err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("get config")
		WriteJSONError(w, http.StatusInternalServerError, "failed to read config")
		return
	}
	if v := r.FormValue("auto_load"); v != "" {
		cfg.AutoLoad = parseBoolToggle(v)
	}
	if v := r.FormValue("retention"); v != "" {
		n, parseErr := strconv.ParseUint(strings.TrimSpace(v), 10, 32)
		if parseErr != nil || n == 0 {
			WriteJSONError(w, http.StatusBadRequest, "retention must be a positive integer")
			return
		}
		cfg.RetentionCount = uint32(n)
	}
	if err := h.configStore.Upsert(cfg); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("upsert config")
		WriteJSONError(w, http.StatusInternalServerError, "failed to save config")
		return
	}
	WriteJSON(w, http.StatusOK, AutoLoadToggleResponse{OK: true})
}

// SetPinAPI flips a run's pin state. Body: `pinned=true|false`.
// 404 if the run isn't known to the Manager.
func (h *Handlers) SetPinAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))
	if err := r.ParseForm(); err != nil {
		WriteJSONError(w, http.StatusBadRequest, "invalid form body")
		return
	}
	pinned := parseBoolToggle(r.FormValue("pinned"))
	if err := h.manager.SetPinned(id, pinned); err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
			return
		}
		zerolog.Ctx(r.Context()).Error().Err(err).Stringer("id", id).Msg("set pin")
		WriteJSONError(w, http.StatusInternalServerError, "failed to set pin")
		return
	}
	WriteJSON(w, http.StatusOK, AutoLoadToggleResponse{OK: true})
}

// DiskBudgetResponse exposes the current budget tracker state for the
// dashboard gauge. Bytes throughout so the UI can format consistently.
type DiskBudgetResponse struct {
	CapBytes      uint64 `json:"cap_bytes"`
	UsedBytes     uint64 `json:"used_bytes"`
	ReservedBytes uint64 `json:"reserved_bytes"`
	HeadroomBytes uint64 `json:"headroom_bytes"`
	AvailBytes    uint64 `json:"available_bytes"`
}

// DiskBudgetAPI returns the current Tracker counters. When no budget is
// configured (cap==0) returns zeros — clients can switch off the gauge.
func (h *Handlers) DiskBudgetAPI(w http.ResponseWriter, _ *http.Request) {
	resp := DiskBudgetResponse{}
	if h.tracker != nil {
		resp.CapBytes = h.tracker.Cap()
		resp.UsedBytes = h.tracker.Used()
		resp.ReservedBytes = h.tracker.Reserved()
		resp.HeadroomBytes = h.tracker.Headroom()
		resp.AvailBytes = h.tracker.Available()
	}
	WriteJSON(w, http.StatusOK, resp)
}

// parseBoolToggle interprets a form value that may come from a checkbox
// (Web form "on"/empty), an explicit "true"/"false", or "1"/"0".
func parseBoolToggle(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "on", "true", "1", "yes":
		return true
	default:
		return false
	}
}
