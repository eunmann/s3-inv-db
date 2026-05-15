package templates

import "time"

// FormatETA exposes formatETA for tests in the templates_test package.
func FormatETA(startedAt time.Time, done, total int64) string {
	return formatETA(startedAt, done, total)
}

// ProgressPct exposes progressPct for tests.
func ProgressPct(done, total int64) int { return progressPct(done, total) }

// StageLabel exposes stageLabel for tests.
func StageLabel(stage string) string { return stageLabel(stage) }

// HxValsJSON exposes hxValsJSON for tests.
func HxValsJSON(pairs ...any) (string, error) { return hxValsJSON(pairs...) }

// BrowseURL exposes browseURL for tests.
func BrowseURL(pairs ...any) (string, error) { return browseURL(pairs...) }
