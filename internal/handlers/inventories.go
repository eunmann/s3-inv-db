package handlers

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/go-chi/chi/v5"
)

// RegisterInventoryRequest is the request body for registering a new inventory.
type RegisterInventoryRequest struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Path string `json:"path"`
}

// ListInventoriesAPI returns a JSON list of all inventories.
func (h *Handlers) ListInventoriesAPI(w http.ResponseWriter, _ *http.Request) {
	inventories := h.manager.List()
	WriteJSON(w, http.StatusOK, inventories)
}

// RegisterInventoryAPI registers a new inventory.
func (h *Handlers) RegisterInventoryAPI(w http.ResponseWriter, r *http.Request) {
	var req RegisterInventoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSONError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.ID == "" {
		WriteJSONError(w, http.StatusBadRequest, "id is required")
		return
	}
	if req.Name == "" {
		WriteJSONError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Path == "" {
		WriteJSONError(w, http.StatusBadRequest, "path is required")
		return
	}

	if err := h.manager.Register(req.ID, req.Name, req.Path); err != nil {
		if errors.Is(err, inventory.ErrAlreadyExists) {
			WriteJSONError(w, http.StatusConflict, "inventory already exists")
			return
		}
		h.logger.Error().Err(err).Msg("failed to register inventory")
		WriteJSONError(w, http.StatusInternalServerError, "failed to register inventory")
		return
	}

	info, _ := h.manager.Get(req.ID)
	WriteJSON(w, http.StatusCreated, info)
}

// GetInventoryAPI returns a single inventory by ID.
func (h *Handlers) GetInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	info, exists := h.manager.Get(id)
	if !exists {
		WriteJSONError(w, http.StatusNotFound, "inventory not found")
		return
	}

	WriteJSON(w, http.StatusOK, info)
}

// LoadInventoryAPI loads an inventory index.
func (h *Handlers) LoadInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if err := h.manager.Load(r.Context(), id); err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
			return
		}
		if errors.Is(err, inventory.ErrInvalidState) {
			WriteJSONError(w, http.StatusConflict, err.Error())
			return
		}
		h.logger.Error().Err(err).Str("id", id).Msg("failed to load inventory")
		WriteJSONError(w, http.StatusInternalServerError, "failed to load inventory: "+err.Error())
		return
	}

	info, _ := h.manager.Get(id)
	WriteJSON(w, http.StatusOK, info)
}

// UnloadInventoryAPI unloads an inventory index.
func (h *Handlers) UnloadInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if err := h.manager.Unload(id); err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
			return
		}
		if errors.Is(err, inventory.ErrInvalidState) {
			WriteJSONError(w, http.StatusConflict, err.Error())
			return
		}
		h.logger.Error().Err(err).Str("id", id).Msg("failed to unload inventory")
		WriteJSONError(w, http.StatusInternalServerError, "failed to unload inventory")
		return
	}

	info, _ := h.manager.Get(id)
	WriteJSON(w, http.StatusOK, info)
}

// DeleteInventoryAPI removes an inventory from the manager.
func (h *Handlers) DeleteInventoryAPI(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if err := h.manager.Remove(id); err != nil {
		if errors.Is(err, inventory.ErrNotFound) {
			WriteJSONError(w, http.StatusNotFound, "inventory not found")
			return
		}
		h.logger.Error().Err(err).Str("id", id).Msg("failed to delete inventory")
		WriteJSONError(w, http.StatusInternalServerError, "failed to delete inventory")
		return
	}

	WriteJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// InventoriesPage renders the inventories HTML page.
func (h *Handlers) InventoriesPage(w http.ResponseWriter, _ *http.Request) {
	inventories := h.manager.List()

	data := map[string]interface{}{
		"Title":       "Inventories",
		"Inventories": inventories,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.Render(w, "inventories.html", data); err != nil {
		h.logger.Error().Err(err).Msg("failed to render inventories page")
		http.Error(w, "failed to render page", http.StatusInternalServerError)
	}
}

// InventoryRowPartial renders an inventory row partial for HTMX.
func (h *Handlers) InventoryRowPartial(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	info, exists := h.manager.Get(id)
	if !exists {
		WriteJSONError(w, http.StatusNotFound, "inventory not found")
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.renderer.RenderPartial(w, "inventory_row.html", info); err != nil {
		h.logger.Error().Err(err).Msg("failed to render inventory row")
		http.Error(w, "failed to render partial", http.StatusInternalServerError)
	}
}
