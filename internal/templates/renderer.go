// Package templates provides HTML template rendering with embedded templates.
package templates

import (
	"embed"
	"fmt"
	"html/template"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)

//go:embed templates/*.html templates/partials/*.html
var embeddedTemplates embed.FS

// Renderer manages HTML template rendering.
type Renderer struct {
	base    *template.Template // Base templates (layout + partials)
	pages   map[string]string  // Page template content by name
	devMode bool
	rootDir string
	funcMap template.FuncMap
}

// New creates a new template renderer.
// If devMode is true, templates are reloaded from disk on each render for development.
func New(devMode bool) (*Renderer, error) {
	r := &Renderer{
		devMode: devMode,
		rootDir: "internal/templates",
		pages:   make(map[string]string),
		funcMap: FuncMap(),
	}

	if err := r.loadTemplates(); err != nil {
		return nil, err
	}

	return r, nil
}

// FuncMap returns the template function map.
func FuncMap() template.FuncMap {
	return template.FuncMap{
		"formatBytes":       humanfmt.BytesUint64,
		"formatCount":       humanfmt.CountUint64,
		"formatCost":        pricing.FormatCost,
		"formatCostDollars": pricing.FormatCostDollars,
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
		"stateClass": func(state string) string {
			switch state {
			case "loaded":
				return "bg-green-100 text-green-800"
			case "pending":
				return "bg-yellow-100 text-yellow-800"
			case "parsing":
				return "bg-blue-100 text-blue-800"
			case "error":
				return "bg-red-100 text-red-800"
			case "unloaded":
				return "bg-gray-100 text-gray-800"
			default:
				return "bg-gray-100 text-gray-800"
			}
		},
		"add": func(a, b int) int {
			return a + b
		},
		"mul": func(a, b int) int {
			return a * b
		},
	}
}

func (r *Renderer) loadTemplates() error {
	r.base = template.New("base").Funcs(r.funcMap)
	r.pages = make(map[string]string)

	if r.devMode {
		return r.loadFromDisk()
	}

	return r.loadFromEmbed()
}

func (r *Renderer) loadFromEmbed() error {
	// Load layout template into base
	layoutContent, err := embeddedTemplates.ReadFile("templates/layout.html")
	if err != nil {
		return fmt.Errorf("read layout: %w", err)
	}
	if _, err := r.base.Parse(string(layoutContent)); err != nil {
		return fmt.Errorf("parse layout: %w", err)
	}

	// Load partial templates into base
	partialEntries, err := embeddedTemplates.ReadDir("templates/partials")
	if err != nil {
		return fmt.Errorf("read partials dir: %w", err)
	}

	for _, entry := range partialEntries {
		if entry.IsDir() {
			continue
		}
		path := "templates/partials/" + entry.Name()
		content, err := embeddedTemplates.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		_, err = r.base.New(path).Parse(string(content))
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
	}

	// Load page templates into pages map (not base)
	entries, err := embeddedTemplates.ReadDir("templates")
	if err != nil {
		return fmt.Errorf("read templates dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "layout.html" {
			continue
		}
		path := "templates/" + entry.Name()
		content, err := embeddedTemplates.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		r.pages[entry.Name()] = string(content)
	}

	return nil
}

func (r *Renderer) loadFromDisk() error {
	// Load layout template into base
	layoutPath := filepath.Join(r.rootDir, "templates", "layout.html")
	layoutContent, err := os.ReadFile(layoutPath)
	if err != nil {
		return fmt.Errorf("read layout: %w", err)
	}
	if _, err := r.base.Parse(string(layoutContent)); err != nil {
		return fmt.Errorf("parse layout: %w", err)
	}

	// Load partial templates into base
	partialsGlob := filepath.Join(r.rootDir, "templates", "partials", "*.html")
	files, err := filepath.Glob(partialsGlob)
	if err != nil {
		return fmt.Errorf("glob partial templates: %w", err)
	}

	for _, f := range files {
		content, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		name := "templates/partials/" + filepath.Base(f)
		_, err = r.base.New(name).Parse(string(content))
		if err != nil {
			return fmt.Errorf("parse %s: %w", f, err)
		}
	}

	// Load page templates into pages map
	mainGlob := filepath.Join(r.rootDir, "templates", "*.html")
	files, err = filepath.Glob(mainGlob)
	if err != nil {
		return fmt.Errorf("glob main templates: %w", err)
	}

	for _, f := range files {
		name := filepath.Base(f)
		if name == "layout.html" {
			continue
		}
		content, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		r.pages[name] = string(content)
	}

	return nil
}

// Render renders a full page template.
func (r *Renderer) Render(w io.Writer, name string, data interface{}) error {
	if r.devMode {
		if err := r.loadTemplates(); err != nil {
			return fmt.Errorf("reload templates: %w", err)
		}
	}

	pageContent, ok := r.pages[name]
	if !ok {
		return fmt.Errorf("page template %s not found", name)
	}

	// Clone base templates and add page-specific content
	tmpl, err := r.base.Clone()
	if err != nil {
		return fmt.Errorf("clone base template: %w", err)
	}

	if _, err := tmpl.Parse(pageContent); err != nil {
		return fmt.Errorf("parse page template %s: %w", name, err)
	}

	// Execute the page template which calls layout
	if err := tmpl.ExecuteTemplate(w, "layout", data); err != nil {
		return fmt.Errorf("execute template %s: %w", name, err)
	}
	return nil
}

// RenderPartial renders a partial template without layout.
func (r *Renderer) RenderPartial(w io.Writer, name string, data interface{}) error {
	if r.devMode {
		if err := r.loadTemplates(); err != nil {
			return fmt.Errorf("reload templates: %w", err)
		}
	}

	if err := r.base.ExecuteTemplate(w, "templates/partials/"+name, data); err != nil {
		return fmt.Errorf("execute partial template %s: %w", name, err)
	}
	return nil
}
