package handlers_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/handlers"
	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/templates"
	"github.com/go-chi/chi/v5"
)

func postAutoLoad(t *testing.T, router http.Handler, form url.Values) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/configurations/bkt/inv/auto-load", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

// The UI form sends a hidden auto_load=off before the checkbox; an
// unchecked box sends nothing, so auto-load could never be turned off.
// The last value is the checkbox's state.
func TestSetAutoLoadConfigAPI_CheckboxOnAndOff(t *testing.T) {
	h, configs := newWiredHandlersWithConfigs(t, nil)
	router := chi.NewRouter()
	router.Post("/api/configurations/{src}/{name}/auto-load", h.SetAutoLoadConfigAPI)

	postAutoLoad(t, router, url.Values{"auto_load": {"off", "on"}, "retention": {"3"}})
	cfg, err := configs.Get(t.Context(), "bkt", "inv")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AutoLoad || cfg.RetentionCount != 3 {
		t.Fatalf("after checking: %+v, want auto_load on, retention 3", cfg)
	}

	postAutoLoad(t, router, url.Values{"auto_load": {"off"}, "retention": {"3"}})
	if cfg, _ = configs.Get(t.Context(), "bkt", "inv"); cfg.AutoLoad {
		t.Error("unchecking the box left auto-load on")
	}
}

// Saving the policy must not wipe the poller's state (and vice versa,
// covered in the autoload tests).
func TestSetAutoLoadConfigAPI_KeepsPollState(t *testing.T) {
	h, configs := newWiredHandlersWithConfigs(t, nil)
	router := chi.NewRouter()
	router.Post("/api/configurations/{src}/{name}/auto-load", h.SetAutoLoadConfigAPI)
	if err := configs.SetPolicy(t.Context(), "bkt", "inv", true, 2); err != nil {
		t.Fatal(err)
	}
	if err := configs.RecordPoll(t.Context(), "bkt", "inv", inventory.PollState{LastError: "access denied", FailureCount: 2}); err != nil {
		t.Fatal(err)
	}

	postAutoLoad(t, router, url.Values{"auto_load": {"off", "on"}, "retention": {"4"}})

	cfg, err := configs.Get(t.Context(), "bkt", "inv")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.LastPollError != "access denied" || cfg.PollFailureCount != 2 {
		t.Errorf("poll state lost on policy save: %+v", cfg)
	}
}

// A toggle on a configuration with no stored row and no retention in
// the form takes the server's default retention (#149), not a fixed 2.
func TestSetAutoLoadConfigAPI_NewConfigTakesDefaultRetention(t *testing.T) {
	h, configs := newWiredHandlersWithConfigs(t, nil)
	configs.WithDefaultRetention(4)
	router := chi.NewRouter()
	router.Post("/api/configurations/{src}/{name}/auto-load", h.SetAutoLoadConfigAPI)

	postAutoLoad(t, router, url.Values{"auto_load": {"off", "on"}})
	cfg, err := configs.Get(t.Context(), "bkt", "inv")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AutoLoad || cfg.RetentionCount != 4 {
		t.Fatalf("new config %+v, want auto_load on, retention 4", cfg)
	}
}

func postAutoLoadStatus(t *testing.T, router http.Handler, form url.Values) int {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/configurations/bkt/inv/auto-load", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	return rec.Code
}

// A setting the config file manages can't be changed through the API
// (#149): a different value is refused with 409, the same value
// passes, and a setting the file leaves alone stays editable.
func TestSetAutoLoadConfigAPI_RefusesChangesToManagedSettings(t *testing.T) {
	h, configs := newWiredHandlersWithConfigs(t, nil, handlers.WithManagedConfigs(inventory.ManagedConfigs{"bkt/inv": {AutoLoad: true}}))
	if err := configs.SetPolicy(t.Context(), "bkt", "inv", true, 2); err != nil {
		t.Fatal(err)
	}
	router := chi.NewRouter()
	router.Post("/api/configurations/{src}/{name}/auto-load", h.SetAutoLoadConfigAPI)

	if code := postAutoLoadStatus(t, router, url.Values{"auto_load": {"off"}}); code != http.StatusConflict {
		t.Errorf("turning off a managed auto-load: status %d, want 409", code)
	}
	if code := postAutoLoadStatus(t, router, url.Values{"auto_load": {"on"}, "retention": {"5"}}); code != http.StatusOK {
		t.Errorf("same auto-load, new unmanaged retention: status %d, want 200", code)
	}
	cfg, err := configs.Get(t.Context(), "bkt", "inv")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AutoLoad || cfg.RetentionCount != 5 {
		t.Errorf("config %+v, want auto-load still on, retention 5", cfg)
	}
}

// The inventories page shows managed settings disabled and posts no
// hidden auto_load=off for them, which would read as turning it off.
func TestInventoriesPage_ManagedSettingsAreReadOnly(t *testing.T) {
	r, err := templates.New()
	if err != nil {
		t.Fatal(err)
	}
	render := func(g handlers.InventoryGroup) string {
		t.Helper()
		var buf bytes.Buffer
		data := handlers.InventoriesData{Title: "Inventories", HasDiscovery: true, Groups: []handlers.InventoryGroup{g}}
		if err := r.Render(&buf, "inventories.html", data); err != nil {
			t.Fatal(err)
		}

		return buf.String()
	}
	managed := render(handlers.InventoryGroup{SourceBucket: "bkt", Name: "inv", AutoLoad: true, AutoLoadManaged: true, RetentionManaged: true})
	if strings.Contains(managed, `name="auto_load" value="off"`) {
		t.Error("managed auto-load still posts a hidden auto_load=off")
	}
	if strings.Count(managed, "disabled") < 2 || !strings.Contains(managed, "Set in the config file") {
		t.Error("managed settings aren't disabled and labelled")
	}
	free := render(handlers.InventoryGroup{SourceBucket: "bkt", Name: "inv"})
	if !strings.Contains(free, `name="auto_load" value="off"`) || strings.Contains(free, "Set in the config file") {
		t.Error("unmanaged settings lost their hidden field or gained the managed label")
	}
}
