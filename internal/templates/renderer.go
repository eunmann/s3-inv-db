// Package templates provides HTML template rendering with embedded templates.
package templates

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"net/url"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)

// stateLabel turns internal inventory state names into user-facing
// language. Kept aligned with the inventory.State constants.
func stateLabel(state string) string {
	switch state {
	case "loaded":
		return "Loaded"
	case "not_loaded":
		return "Not loaded"
	case "loading":
		return "Loading…"
	case "error":
		return "Error"
	default:
		return state
	}
}

// formatETA produces a human-readable remaining-time estimate. Returns
// the empty string when an estimate isn't meaningful — no start time,
// no progress yet, or already at/past completion.
func formatETA(startedAt time.Time, done, total int64) string {
	if startedAt.IsZero() || done <= 0 || total <= 0 || done >= total {
		return ""
	}
	elapsed := time.Since(startedAt)
	if elapsed <= 0 {
		return ""
	}
	remaining := time.Duration(float64(elapsed) * float64(total-done) / float64(done))
	return humanfmt.Duration(remaining)
}

// progressPct rounds done/total to a whole-percent integer for display.
// Returns 0 when total is zero so callers can do `{{if gt (progressPct …) 0}}`.
func progressPct(done, total int64) int {
	if total <= 0 || done <= 0 {
		return 0
	}
	if done >= total {
		return 100
	}
	return int(float64(done) * 100.0 / float64(total))
}

// stageLabel renders pipeline phase names from the build pipeline.
func stageLabel(stage string) string {
	switch stage {
	case "preparing":
		return "Preparing"
	case "initializing":
		return "Initializing"
	case "downloading":
		return "Downloading & parsing"
	case "building":
		return "Building index"
	case "done":
		return "Done"
	default:
		return stage
	}
}

//go:embed templates/*.html templates/partials/*.html
var embeddedTemplates embed.FS

// Renderer manages HTML template rendering.
//
// Each page template is fully resolved at load time (layout + every partial +
// the page itself) and stored in its own *template.Template tree. RenderPartial
// uses a separate partials-only tree. We never call Clone — html/template
// forbids cloning a tree after it has been executed, which would break every
// request after the first.
type Renderer struct {
	pages    map[string]*template.Template
	partials *template.Template
	funcMap  template.FuncMap
}

// New creates a new template renderer with the embedded templates parsed.
// HTML hot-reload during local development is handled by Air watching
// templates/*.html files (per .air.toml) — Air rebuilds the binary on
// HTML change, which re-runs the embed.FS load on startup. There's no
// in-process devMode-reload path because there doesn't need to be.
func New() (*Renderer, error) {
	r := &Renderer{funcMap: FuncMap()}
	if err := r.loadTemplates(); err != nil {
		return nil, err
	}
	return r, nil
}

// FuncMap returns the template function map.
func FuncMap() template.FuncMap {
	return template.FuncMap{
		"formatBytes": humanfmt.BytesUint64,
		"formatCount": humanfmt.CountUint64,
		"formatCost":  pricing.FormatCost,
		"formatTime": func(t time.Time) string {
			if t.IsZero() {
				return "-"
			}
			return t.Format(time.RFC3339)
		},
		"formatTimeRelative": func(t time.Time) string {
			if t.IsZero() {
				return "-"
			}
			since := time.Since(t)
			switch {
			case since < time.Minute:
				return "just now"
			case since < time.Hour:
				return fmt.Sprintf("%d min ago", int(since.Minutes()))
			case since < 24*time.Hour:
				return fmt.Sprintf("%d hr ago", int(since.Hours()))
			default:
				return t.Format("Jan 2, 15:04")
			}
		},
		"stateLabel":  stateLabel,
		"stageLabel":  stageLabel,
		"formatETA":   formatETA,
		"progressPct": progressPct,
		"stateClass": func(state string) string {
			switch state {
			case "loaded":
				return "bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300"
			case "not_loaded":
				return "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-300"
			case "loading":
				return "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300"
			case "error":
				return "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300"
			default:
				return "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200"
			}
		},
		"diffStatusClass": func(status string) string {
			switch status {
			case "added":
				return "bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300"
			case "removed":
				return "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300"
			case "changed":
				return "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-300"
			default:
				return "bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300"
			}
		},
		"add": func(a, b int) int {
			return a + b
		},
		"sub": func(a, b int) int {
			return a - b
		},
		"mul": func(a, b int) int {
			return a * b
		},
		"hxVals":    hxValsAttr,
		"browseURL": browseURL,
	}
}

// hxValsAttr emits a full `hx-vals='…'` attribute with JSON-encoded
// values. Each pair is key,value. Values that contain `"`, `\` or `'`
// break the naïve `hx-vals='{"k":"{{.V}}"}'` pattern: html/template
// HTML-escapes `"` to `&#34;` which the browser un-escapes back to a
// raw `"` inside the JSON, corrupting the payload. JSON-marshaling here
// produces the proper `\"` escape, and we additionally replace any `'`
// with the HTML entity so the surrounding single quotes stay intact.
func hxValsAttr(pairs ...any) (template.HTMLAttr, error) {
	if len(pairs)%2 != 0 {
		return "", errors.New("hxVals: expected key,value pairs")
	}
	m := make(map[string]any, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok {
			return "", fmt.Errorf("hxVals: key at position %d is not a string", i)
		}
		m[key] = pairs[i+1]
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("hxVals: marshal: %w", err)
	}
	b = bytes.ReplaceAll(b, []byte("'"), []byte("&#39;"))
	return template.HTMLAttr(`hx-vals='` + string(b) + `'`), nil
}

// Builds a percent-encoded /browse URL from key/value pairs. Required
// because html/template only auto-encodes URL values inside a fixed set
// of attributes (href, src, action, …) and `hx-push-url` is not one of
// them, so a raw `{{.Prefix}}` interpolation lets `&`, `?`, `#`, or
// spaces in a prefix break the URL.
func browseURL(pairs ...any) (string, error) {
	if len(pairs)%2 != 0 {
		return "", errors.New("browseURL: expected key,value pairs")
	}
	u := url.URL{Path: "/browse"}
	q := u.Query()
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok {
			return "", fmt.Errorf("browseURL: key at position %d is not a string", i)
		}
		v := fmt.Sprint(pairs[i+1])
		if v == "" {
			continue
		}
		q.Set(key, v)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (r *Renderer) loadTemplates() error {
	layoutSrc, partialSrcs, pageSrcs, err := readTemplates(embeddedTemplates)
	if err != nil {
		return err
	}

	// Partials-only tree for RenderPartial.
	partials := template.New("partials").Funcs(r.funcMap)
	for name, src := range partialSrcs {
		if _, err := partials.New(name).Parse(src); err != nil {
			return fmt.Errorf("parse partial %s: %w", name, err)
		}
	}
	r.partials = partials

	// One fully-resolved tree per page: layout + every partial + the page.
	r.pages = make(map[string]*template.Template, len(pageSrcs))
	for pageName, pageSrc := range pageSrcs {
		t := template.New(pageName).Funcs(r.funcMap)
		if _, err := t.Parse(layoutSrc); err != nil {
			return fmt.Errorf("parse layout for %s: %w", pageName, err)
		}
		for partialName, partialSrc := range partialSrcs {
			if _, err := t.New(partialName).Parse(partialSrc); err != nil {
				return fmt.Errorf("parse partial %s for page %s: %w", partialName, pageName, err)
			}
		}
		if _, err := t.Parse(pageSrc); err != nil {
			return fmt.Errorf("parse page %s: %w", pageName, err)
		}
		r.pages[pageName] = t
	}

	return nil
}

// readTemplates loads layout + partials + page sources from any fs.FS
// rooted at the project layout (templates/, templates/partials/).
func readTemplates(src fs.FS) (layoutSrc string, partials, pages map[string]string, err error) {
	layoutBytes, err := fs.ReadFile(src, "templates/layout.html")
	if err != nil {
		return "", nil, nil, fmt.Errorf("read layout: %w", err)
	}
	partials, err = readDirFiles(src, "templates/partials", false)
	if err != nil {
		return "", nil, nil, err
	}
	pages, err = readDirFiles(src, "templates", true)
	if err != nil {
		return "", nil, nil, err
	}
	delete(pages, "layout.html")
	return string(layoutBytes), partials, pages, nil
}

// Enumerates a directory and returns a map of either path→content
// (full path key) or basename→content. The partials code path needs
// full paths (used as the parsed template name); pages key by basename.
func readDirFiles(src fs.FS, dir string, basenameKey bool) (map[string]string, error) {
	entries, err := fs.ReadDir(src, dir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}
	out := make(map[string]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := dir + "/" + entry.Name()
		content, err := fs.ReadFile(src, path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		key := path
		if basenameKey {
			key = entry.Name()
		}
		out[key] = string(content)
	}
	return out, nil
}

// Render renders a full page template.
func (r *Renderer) Render(w io.Writer, name string, data interface{}) error {
	t, ok := r.pages[name]
	if !ok {
		return fmt.Errorf("page template %s not found", name)
	}
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		return fmt.Errorf("execute template %s: %w", name, err)
	}
	return nil
}

// RenderPartial renders a partial template without layout.
func (r *Renderer) RenderPartial(w io.Writer, name string, data interface{}) error {
	if err := r.partials.ExecuteTemplate(w, "templates/partials/"+name, data); err != nil {
		return fmt.Errorf("execute partial template %s: %w", name, err)
	}
	return nil
}
