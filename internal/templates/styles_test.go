package templates

import (
	"strings"
	"testing"
)

// TestTailwindClasses_FromGoHelpersCompiled: every Tailwind class
// returned by funcmap helpers must appear in the compiled stylesheet.
// Regression guard for the chip dark-mode bug — a helper file missing
// from tailwind.config.js content paths silently drops its variants.
func TestTailwindClasses_FromGoHelpersCompiled(t *testing.T) {
	css := string(TailwindCSS())
	if css == "" {
		t.Fatal("TailwindCSS() returned empty; run `make css`")
	}

	stateClass, ok := FuncMap()["stateClass"].(func(string) string)
	if !ok {
		t.Fatal("funcmap entry stateClass is missing or has the wrong signature")
	}

	// Literals (not inventory.State constants) to avoid an import cycle.
	states := []string{"loaded", "not_loaded", "loading", "error"}

	for _, state := range states {
		classes := strings.Fields(stateClass(state))
		for _, cls := range classes {
			if !cssHasClass(css, cls) {
				t.Errorf("compiled CSS is missing a rule for stateClass(%q) class %q — "+
					"check tailwind.config.js content paths include the Go file that emits it",
					state, cls)
			}
		}
	}
}

// cssHasClass reports whether the compiled stylesheet contains a rule
// selector for the given Tailwind class. `:` and `/` are backslash-
// escaped in Tailwind selectors (`.dark\:bg-yellow-900\/40`).
func cssHasClass(css, class string) bool {
	r := strings.NewReplacer(":", `\:`, "/", `\/`)
	return strings.Contains(css, "."+r.Replace(class))
}
