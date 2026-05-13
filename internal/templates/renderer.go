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
//
// Each page template is fully resolved at load time (layout + every partial +
// the page itself) and stored in its own *template.Template tree. RenderPartial
// uses a separate partials-only tree. We never call Clone — html/template
// forbids cloning a tree after it has been executed, which would break every
// request after the first.
type Renderer struct {
	pages    map[string]*template.Template
	partials *template.Template
	devMode  bool
	rootDir  string
	funcMap  template.FuncMap
}

// New creates a new template renderer.
// If devMode is true, templates are reloaded from disk on each render for
// development. The default rootDir is "internal/templates" relative to the
// process working directory; use NewWithRootDir to override.
func New(devMode bool) (*Renderer, error) {
	return NewWithRootDir(devMode, "internal/templates")
}

// NewWithRootDir is like New but lets the caller pick the on-disk rootDir
// used when devMode is true. Tests use this to point at a temp directory.
func NewWithRootDir(devMode bool, rootDir string) (*Renderer, error) {
	r := &Renderer{
		devMode: devMode,
		rootDir: rootDir,
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
		"sub": func(a, b int) int {
			return a - b
		},
		"mul": func(a, b int) int {
			return a * b
		},
	}
}

func (r *Renderer) loadTemplates() error {
	var (
		layoutSrc   string
		partialSrcs map[string]string
		pageSrcs    map[string]string
		err         error
	)
	if r.devMode {
		layoutSrc, partialSrcs, pageSrcs, err = r.readFromDisk()
	} else {
		layoutSrc, partialSrcs, pageSrcs, err = r.readFromEmbed()
	}
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

// readFromEmbed loads template sources from the embedded FS.
func (r *Renderer) readFromEmbed() (layoutSrc string, partials, pages map[string]string, err error) {
	layoutContent, err := embeddedTemplates.ReadFile("templates/layout.html")
	if err != nil {
		return "", nil, nil, fmt.Errorf("read layout: %w", err)
	}

	partialEntries, err := embeddedTemplates.ReadDir("templates/partials")
	if err != nil {
		return "", nil, nil, fmt.Errorf("read partials dir: %w", err)
	}
	partialSrcs := make(map[string]string, len(partialEntries))
	for _, entry := range partialEntries {
		if entry.IsDir() {
			continue
		}
		path := "templates/partials/" + entry.Name()
		content, err := embeddedTemplates.ReadFile(path)
		if err != nil {
			return "", nil, nil, fmt.Errorf("read %s: %w", path, err)
		}
		partialSrcs[path] = string(content)
	}

	entries, err := embeddedTemplates.ReadDir("templates")
	if err != nil {
		return "", nil, nil, fmt.Errorf("read templates dir: %w", err)
	}
	pageSrcs := make(map[string]string, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "layout.html" {
			continue
		}
		path := "templates/" + entry.Name()
		content, err := embeddedTemplates.ReadFile(path)
		if err != nil {
			return "", nil, nil, fmt.Errorf("read %s: %w", path, err)
		}
		pageSrcs[entry.Name()] = string(content)
	}

	return string(layoutContent), partialSrcs, pageSrcs, nil
}

// readFromDisk loads template sources from r.rootDir.
func (r *Renderer) readFromDisk() (layoutSrc string, partials, pages map[string]string, err error) {
	layoutPath := filepath.Join(r.rootDir, "templates", "layout.html")
	layoutContent, err := os.ReadFile(layoutPath)
	if err != nil {
		return "", nil, nil, fmt.Errorf("read layout: %w", err)
	}

	partialsGlob := filepath.Join(r.rootDir, "templates", "partials", "*.html")
	partialFiles, err := filepath.Glob(partialsGlob)
	if err != nil {
		return "", nil, nil, fmt.Errorf("glob partials: %w", err)
	}
	partialSrcs := make(map[string]string, len(partialFiles))
	for _, f := range partialFiles {
		content, err := os.ReadFile(f)
		if err != nil {
			return "", nil, nil, fmt.Errorf("read %s: %w", f, err)
		}
		name := "templates/partials/" + filepath.Base(f)
		partialSrcs[name] = string(content)
	}

	mainGlob := filepath.Join(r.rootDir, "templates", "*.html")
	pageFiles, err := filepath.Glob(mainGlob)
	if err != nil {
		return "", nil, nil, fmt.Errorf("glob pages: %w", err)
	}
	pageSrcs := make(map[string]string, len(pageFiles))
	for _, f := range pageFiles {
		name := filepath.Base(f)
		if name == "layout.html" {
			continue
		}
		content, err := os.ReadFile(f)
		if err != nil {
			return "", nil, nil, fmt.Errorf("read %s: %w", f, err)
		}
		pageSrcs[name] = string(content)
	}

	return string(layoutContent), partialSrcs, pageSrcs, nil
}

// Render renders a full page template.
func (r *Renderer) Render(w io.Writer, name string, data interface{}) error {
	if r.devMode {
		if err := r.loadTemplates(); err != nil {
			return fmt.Errorf("reload templates: %w", err)
		}
	}

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
	if r.devMode {
		if err := r.loadTemplates(); err != nil {
			return fmt.Errorf("reload templates: %w", err)
		}
	}

	if err := r.partials.ExecuteTemplate(w, "templates/partials/"+name, data); err != nil {
		return fmt.Errorf("execute partial template %s: %w", name, err)
	}
	return nil
}
