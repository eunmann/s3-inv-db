// Package templates provides HTML template rendering with embedded templates.
package templates

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"maps"
	"net/url"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/pricing"
)

// State name literals shared by stateLabel / stateClass / stateBorderClass.
// Kept aligned with the inventory.State constants but as plain strings
// here so renderer.go doesn't import the inventory package.
const (
	stateLoaded    = "loaded"
	stateNotLoaded = "not_loaded"
	stateLoading   = "loading"
	stateErrored   = "error"
)

// stateLabel turns internal inventory state names into user-facing
// language. Kept aligned with the inventory.State constants.
func stateLabel(state string) string {
	switch state {
	case stateLoaded:
		return "Loaded"
	case stateNotLoaded:
		return "Not loaded"
	case stateLoading:
		return "Loading…"
	case stateErrored:
		return "Error"
	default:
		return state
	}
}

// formatETAAt produces a human-readable remaining-time estimate
// relative to the provided "now". Returns the empty string when an
// estimate isn't meaningful — no start time, no progress yet, or
// already at/past completion. The clock is parameterised so golden
// tests can pin time.
func formatETAAt(now, startedAt time.Time, done, total int64) string {
	if startedAt.IsZero() || done <= 0 || total <= 0 || done >= total {
		return ""
	}
	elapsed := now.Sub(startedAt)
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

// stageDescription is the tooltip body for a pipeline phase.
func stageDescription(stage string) string {
	switch stage {
	case "preparing":
		return "Wiping any stale cache and reserving the build directory"
	case "initializing":
		return "Starting the build pipeline workers"
	case "downloading":
		return "Streaming inventory chunks from S3 and aggregating prefixes in memory"
	case "building":
		return "Merging spilled run files and writing the on-disk index + MPHF"
	case "done":
		return "Index materialised on disk and opened in memory"
	default:
		return ""
	}
}

// durationFromNs renders a nanosecond count as a humanised duration.
// Returns "" for non-positive inputs so templates can hide empty slots.
func durationFromNs(ns int64) string {
	if ns <= 0 {
		return ""
	}

	return humanfmt.Duration(time.Duration(ns))
}

// elapsedSince renders time-since-t as a humanised duration. Returns ""
// for zero times or negative deltas so templates can hide empty slots.
func elapsedSince(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	d := time.Since(t)
	if d <= 0 {
		return ""
	}

	return humanfmt.Duration(d)
}

// truncate clips s to the first n runes and appends "…" when it had
// to drop anything. Returns s unchanged for n <= 0 or when s already
// fits. Used by row templates to keep long error messages on one line.
func truncate(n int, s string) string {
	if n <= 0 {
		return s
	}
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}

	return string(runes[:n]) + "…"
}

// firstLine returns the substring of s up to the first newline (or
// the whole string if there is none). Errors from build pipelines are
// often multi-line stack traces; the row only wants the topmost.
func firstLine(s string) string {
	for i, r := range s {
		if r == '\n' || r == '\r' {
			return s[:i]
		}
	}

	return s
}

// StageLabel exposes stageLabel to other packages (drawer view assembly).
func StageLabel(stage string) string { return stageLabel(stage) }

// StageDescription exposes stageDescription to other packages.
func StageDescription(stage string) string { return stageDescription(stage) }

// DurationFromNs exposes durationFromNs to other packages.
func DurationFromNs(ns int64) string { return durationFromNs(ns) }

// tierLabel converts a raw S3 storage-class identifier (e.g.
// "INTELLIGENT_TIERING_FREQUENT_SMALL") into a friendlier label
// ("Intelligent-Tiering Frequent (< 128 KiB)") for UI rendering.
// Unknown tiers fall back to the raw name.
func tierLabel(raw string) string {
	switch raw {
	case "STANDARD":
		return "Standard"
	case "STANDARD_IA":
		return "Standard-IA"
	case "ONEZONE_IA":
		return "One Zone-IA"
	case "GLACIER_IR":
		return "Glacier Instant Retrieval"
	case "GLACIER":
		return "Glacier Flexible Retrieval"
	case "DEEP_ARCHIVE":
		return "Glacier Deep Archive"
	case "REDUCED_REDUNDANCY":
		return "Reduced Redundancy"
	case "INTELLIGENT_TIERING_FREQUENT":
		return "Intelligent-Tiering Frequent"
	case "INTELLIGENT_TIERING_INFREQUENT":
		return "Intelligent-Tiering Infrequent"
	case "INTELLIGENT_TIERING_ARCHIVE_INSTANT":
		return "Intelligent-Tiering Archive Instant"
	case "INTELLIGENT_TIERING_ARCHIVE":
		return "Intelligent-Tiering Archive"
	case "INTELLIGENT_TIERING_DEEP_ARCHIVE":
		return "Intelligent-Tiering Deep Archive"
	case "INTELLIGENT_TIERING_FREQUENT_SMALL":
		return "Intelligent-Tiering Frequent (< 128 KiB)"
	}

	return raw
}

//go:embed templates/*.html templates/partials/*.html
var embeddedTemplates embed.FS

// Sentinel errors for template helper argument validation.
var (
	errHxValsOddPairs    = errors.New("hxVals: expected key,value pairs")
	errHxValsKeyType     = errors.New("hxVals: key is not a string")
	errBrowseURLOddPairs = errors.New("browseURL: expected key,value pairs")
	errBrowseURLKeyType  = errors.New("browseURL: key is not a string")
	errPageNotFound      = errors.New("page template not found")
)

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
	now      func() time.Time
}

// Option configures a Renderer at construction time.
type Option func(*Renderer)

// New creates a new template renderer with the embedded templates parsed.
// HTML hot-reload during local development is handled by Air watching
// templates/*.html files (per .air.toml) — Air rebuilds the binary on
// HTML change, which re-runs the embed.FS load on startup. There's no
// in-process devMode-reload path because there doesn't need to be.
func New(opts ...Option) (*Renderer, error) {
	r := &Renderer{now: time.Now}
	for _, opt := range opts {
		opt(r)
	}
	r.funcMap = r.buildFuncMap()
	if err := r.loadTemplates(); err != nil {
		return nil, err
	}

	return r, nil
}

// buildFuncMap composes the per-domain func sub-maps and binds the
// time-aware helpers to the renderer's clock.
func (r *Renderer) buildFuncMap() template.FuncMap {
	out := template.FuncMap{}
	for _, sub := range []template.FuncMap{
		r.formatFuncs(),
		stateFuncs(),
		compareFuncs(),
		arithFuncs(),
		htmxFuncs(),
	} {
		maps.Copy(out, sub)
	}

	return out
}

// FuncMap returns a default-clock function map. Kept for tests and
// callers that don't need the time-injection plumbing.
func FuncMap() template.FuncMap {
	out := template.FuncMap{}
	for _, sub := range []template.FuncMap{
		(&Renderer{now: time.Now}).formatFuncs(),
		stateFuncs(),
		compareFuncs(),
		arithFuncs(),
		htmxFuncs(),
	} {
		maps.Copy(out, sub)
	}

	return out
}

// formatFuncs covers byte / count / time / cost / tier label
// rendering. Receiver-bound so the time helpers can read the
// renderer's clock (defaults to time.Now).
func (r *Renderer) formatFuncs() template.FuncMap {
	now := r.now
	if now == nil {
		now = time.Now
	}

	return template.FuncMap{
		"formatBytes":      humanfmt.BytesUint64,
		"formatBytesInt64": humanfmt.Bytes,
		"formatCount":      humanfmt.CountUint64,
		"formatCost":       pricing.FormatCost,
		"formatTime": func(t time.Time) string {
			if t.IsZero() {
				return "-"
			}

			return t.Format(time.RFC3339)
		},
		"formatTimeRelative": func(t time.Time) string { return formatTimeRelativeAt(now(), t) },
		"formatETA": func(startedAt time.Time, done, total int64) string {
			return formatETAAt(now(), startedAt, done, total)
		},
		"progressPct": progressPct,
		"tierLabel":   tierLabel,
		"truncate":    truncate,
		"firstLine":   firstLine,
	}
}

// stateFuncs handles inventory state labels and the badge/border colors
// the page renders alongside them.
func stateFuncs() template.FuncMap {
	return template.FuncMap{
		"stateLabel":       stateLabel,
		"stageLabel":       stageLabel,
		"stageDescription": stageDescription,
		"elapsedSince":     elapsedSince,
		"stateClass": func(state string) string {
			switch state {
			case stateLoaded:
				return "bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300"
			case stateNotLoaded:
				return "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-300"
			case stateLoading:
				return "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300"
			case stateErrored:
				return "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300"
			default:
				return "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200"
			}
		},
		"stateBorderClass": func(state string) string {
			switch state {
			case stateLoaded:
				return "border-l-4 border-green-500 dark:border-green-400"
			case stateNotLoaded:
				return "border-l-4 border-yellow-400 dark:border-yellow-500"
			case stateLoading:
				return "border-l-4 border-blue-500 dark:border-blue-400"
			case stateErrored:
				return "border-l-4 border-red-500 dark:border-red-400"
			default:
				return "border-l-4 border-gray-300 dark:border-gray-600"
			}
		},
	}
}

// compareFuncs covers the compare-page row badges.
func compareFuncs() template.FuncMap {
	return template.FuncMap{
		"compareStatusClass": func(status string) string {
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
	}
}

// arithFuncs are pure template arithmetic helpers (pagination math etc).
func arithFuncs() template.FuncMap {
	return template.FuncMap{
		"add": func(a, b int) int { return a + b },
		"sub": func(a, b int) int { return a - b },
		"mul": func(a, b int) int { return a * b },
	}
}

// htmxFuncs covers the htmx-specific attribute helpers.
func htmxFuncs() template.FuncMap {
	return template.FuncMap{
		"hxValsJSON": hxValsJSON,
		"browseURL":  browseURL,
	}
}

// formatTimeRelativeAt is formatTimeRelative parameterised on "now"
// so golden tests can pin time.
func formatTimeRelativeAt(now, t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	since := now.Sub(t)
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
}

// hxValsJSON returns a JSON object literal for use as an `hx-vals`
// attribute value. Templates embed it inside a double-quoted attribute:
//
//	<button hx-vals="{{hxValsJSON "k" .V}}">
//
// html/template's attribute-context escaping turns the JSON's literal
// `"` into `&#34;`. The browser HTML-decodes attribute values before
// scripts read them, so HTMX sees the original JSON and parses it
// cleanly. No template.HTMLAttr cast is needed.
//
// Json.Marshal also escapes `<`, `>`, and `&` to JSON unicode escapes
// (<, >, &) by default, so an attacker-controlled value
// cannot break out of the attribute or close the surrounding tag.
func hxValsJSON(pairs ...any) (string, error) {
	if len(pairs)%2 != 0 {
		return "", errHxValsOddPairs
	}
	m := make(map[string]any, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok {
			return "", fmt.Errorf("%w (position %d)", errHxValsKeyType, i)
		}
		m[key] = pairs[i+1]
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("hxVals: marshal: %w", err)
	}

	return string(b), nil
}

// Builds a percent-encoded /browse URL from key/value pairs. Required
// because html/template only auto-encodes URL values inside a fixed set
// of attributes (href, src, action, …) and `hx-push-url` is not one of
// them, so a raw `{{.Prefix}}` interpolation lets `&`, `?`, `#`, or
// spaces in a prefix break the URL.
func browseURL(pairs ...any) (string, error) {
	if len(pairs)%2 != 0 {
		return "", errBrowseURLOddPairs
	}
	u := url.URL{Path: "/browse"}
	q := u.Query()
	for i := 0; i < len(pairs); i += 2 {
		key, ok := pairs[i].(string)
		if !ok {
			return "", fmt.Errorf("%w (position %d)", errBrowseURLKeyType, i)
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
	srcs, err := readTemplates(embeddedTemplates)
	if err != nil {
		return err
	}

	// Partials-only tree for RenderPartial.
	partials := template.New("partials").Funcs(r.funcMap)
	for name, src := range srcs.Partials {
		if _, err := partials.New(name).Parse(src); err != nil {
			return fmt.Errorf("parse partial %s: %w", name, err)
		}
	}
	r.partials = partials

	// One fully-resolved tree per page: layout + every partial + the page.
	r.pages = make(map[string]*template.Template, len(srcs.Pages))
	for pageName, pageSrc := range srcs.Pages {
		t := template.New(pageName).Funcs(r.funcMap)
		if _, err := t.Parse(srcs.Layout); err != nil {
			return fmt.Errorf("parse layout for %s: %w", pageName, err)
		}
		for partialName, partialSrc := range srcs.Partials {
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

// templateSources groups the three text bundles loaded from disk: the
// layout, the keyed partials, and the keyed pages. Returned together
// from readTemplates so loadTemplates can wire them up.
type templateSources struct {
	Partials map[string]string
	Pages    map[string]string
	Layout   string
}

// readTemplates loads layout + partials + page sources from any fs.FS
// rooted at the project layout (templates/, templates/partials/).
func readTemplates(src fs.FS) (templateSources, error) {
	layoutBytes, err := fs.ReadFile(src, "templates/layout.html")
	if err != nil {
		return templateSources{}, fmt.Errorf("read layout: %w", err)
	}
	partials, err := readDirFiles(src, "templates/partials", false)
	if err != nil {
		return templateSources{}, err
	}
	pages, err := readDirFiles(src, "templates", true)
	if err != nil {
		return templateSources{}, err
	}
	delete(pages, "layout.html")

	return templateSources{
		Layout:   string(layoutBytes),
		Partials: partials,
		Pages:    pages,
	}, nil
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
func (r *Renderer) Render(w io.Writer, name string, data any) error {
	t, ok := r.pages[name]
	if !ok {
		return fmt.Errorf("%w: %s", errPageNotFound, name)
	}
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		return fmt.Errorf("execute template %s: %w", name, err)
	}

	return nil
}

// RenderPartial renders a partial template without layout.
func (r *Renderer) RenderPartial(w io.Writer, name string, data any) error {
	if err := r.partials.ExecuteTemplate(w, "templates/partials/"+name, data); err != nil {
		return fmt.Errorf("execute partial template %s: %w", name, err)
	}

	return nil
}
