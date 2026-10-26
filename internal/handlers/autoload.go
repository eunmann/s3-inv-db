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

type autoLoadToggleResponse struct {
	OK bool `json:"ok"`
}

// SetAutoLoadConfigAPI sets auto_load and (optionally) retention for
// one inventory configuration. Form body: auto_load, retention.
func (h *Handlers) SetAutoLoadConfigAPI(w http.ResponseWriter, r *http.Request) {
	src := chi.URLParam(r, "src")
	name := chi.URLParam(r, "name")
	if src == "" || name == "" {
		configError(w, r, http.StatusBadRequest, "source and name are required")

		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxFormBodyBytes)
	if err := r.ParseForm(); err != nil {
		if bodyTooLarge(err) {
			configError(w, r, http.StatusRequestEntityTooLarge, "request body too large")

			return
		}
		configError(w, r, http.StatusBadRequest, "invalid form body")

		return
	}
	cfg, err := h.configStore.Get(r.Context(), src, name)
	if errors.Is(err, inventory.ErrStoreNotFound) {
		cfg = inventory.Config{Source: src, Name: name, RetentionCount: h.configStore.DefaultRetention()}
	} else if err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("get config")
		configError(w, r, http.StatusInternalServerError, "failed to read config")

		return
	}
	managed := h.managed[cfg.ConfigID()]
	// The UI form sends a hidden auto_load=off before the checkbox, since
	// an unchecked box sends nothing; the last value is the box's state.
	if vs := r.Form["auto_load"]; len(vs) > 0 {
		v := parseBoolToggle(vs[len(vs)-1])
		if managed.AutoLoad && v != cfg.AutoLoad {
			configError(w, r, http.StatusConflict, "auto-load for "+cfg.ConfigID()+" is set in the config file")

			return
		}
		cfg.AutoLoad = v
	}
	if v := r.FormValue("retention"); v != "" {
		n, parseErr := strconv.ParseUint(strings.TrimSpace(v), 10, 32)
		if parseErr != nil || n == 0 {
			configError(w, r, http.StatusBadRequest, "retention must be a positive integer")

			return
		}
		if managed.Retention && n != uint64(cfg.RetentionCount) {
			configError(w, r, http.StatusConflict, "retention for "+cfg.ConfigID()+" is set in the config file")

			return
		}
		cfg.RetentionCount = uint32(n)
	}
	if err := h.configStore.SetPolicy(r.Context(), src, name, cfg.AutoLoad, cfg.RetentionCount); err != nil {
		zerolog.Ctx(r.Context()).Error().Err(err).Msg("set config policy")
		configError(w, r, http.StatusInternalServerError, "failed to save config")

		return
	}
	if formNavigation(r) {
		http.Redirect(w, r, "/inventories", http.StatusSeeOther)

		return
	}
	WriteJSON(w, http.StatusOK, autoLoadToggleResponse{OK: true})
}

// formNavigation: a browser submitted the inventories page's auto-load
// form itself, without htmx (scripts off, Enter in the retention box),
// so it navigates to the response and needs a page, not JSON.
func formNavigation(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "" && strings.Contains(r.Header.Get("Accept"), "text/html")
}

// configError answers a refused auto-load setting: as text a browser
// shows for a form navigation, else as the API's JSON error.
func configError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	if formNavigation(r) {
		http.Error(w, "Not saved: "+msg, status)

		return
	}
	WriteJSONError(w, status, msg)
}

// SetPinAPI sets a run's pin state. Form body: pinned.
func (h *Handlers) SetPinAPI(w http.ResponseWriter, r *http.Request) {
	id := inventory.ID(chi.URLParam(r, "id"))
	r.Body = http.MaxBytesReader(w, r.Body, maxFormBodyBytes)
	if err := r.ParseForm(); err != nil {
		if bodyTooLarge(err) {
			WriteJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")

			return
		}
		WriteJSONError(w, http.StatusBadRequest, "invalid form body")

		return
	}
	pinned := parseBoolToggle(r.FormValue("pinned"))
	if err := h.manager.SetPinned(r.Context(), id, pinned); err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")

			return
		}
		zerolog.Ctx(r.Context()).Error().Err(err).Stringer("id", id).Msg("set pin")
		WriteJSONError(w, http.StatusInternalServerError, "failed to set pin")

		return
	}
	WriteJSON(w, http.StatusOK, autoLoadToggleResponse{OK: true})
}

type DiskBudgetResponse struct {
	CapBytes      uint64 `json:"cap_bytes"`
	UsedBytes     uint64 `json:"used_bytes"`
	ReservedBytes uint64 `json:"reserved_bytes"`
	HeadroomBytes uint64 `json:"headroom_bytes"`
	AvailBytes    uint64 `json:"available_bytes"`
}

// DiskBudgetAPI returns the current Tracker counters; zeros when no budget is set.
func (h *Handlers) DiskBudgetAPI(w http.ResponseWriter, _ *http.Request) {
	WriteJSON(w, http.StatusOK, DiskBudgetResponse{
		CapBytes:      h.tracker.Cap(),
		UsedBytes:     h.tracker.Used(),
		ReservedBytes: h.tracker.Reserved(),
		HeadroomBytes: h.tracker.Headroom(),
		AvailBytes:    h.tracker.Available(),
	})
}

func parseBoolToggle(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "on", trueLiteral, "1", "yes":
		return true
	default:
		return false
	}
}
