package templates_test

import (
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/templates"
)

func TestDeltaClass_RedGrowsGreenShrinks(t *testing.T) {
	deltaClass, ok := templates.FuncMap()["deltaClass"].(func(int) string)
	if !ok {
		t.Fatal("deltaClass is not a func(int) string")
	}
	tests := []struct {
		sign int
		want string
	}{
		{1, "rose"},
		{-1, "emerald"},
		{0, "gray"},
	}
	for _, tt := range tests {
		got := deltaClass(tt.sign)
		if !strings.Contains(got, "text-"+tt.want+"-") || !strings.Contains(got, "dark:text-"+tt.want+"-") {
			t.Errorf("deltaClass(%d) = %q, want %s in light and dark", tt.sign, got, tt.want)
		}
	}
}

func TestCompareStatusClass_MatchesDeltaColours(t *testing.T) {
	statusClass, ok := templates.FuncMap()["compareStatusClass"].(func(string) string)
	if !ok {
		t.Fatal("compareStatusClass is not a func(string) string")
	}
	if got := statusClass("added"); !strings.Contains(got, "rose") {
		t.Errorf("added badge = %q, want rose (growth)", got)
	}
	if got := statusClass("removed"); !strings.Contains(got, "emerald") {
		t.Errorf("removed badge = %q, want emerald (shrinkage)", got)
	}
}
