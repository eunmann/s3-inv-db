package templates

import "time"

// FormatETA exposes the time-bound ETA helper for tests, using
// time.Now so existing tests don't have to pass a clock.
func FormatETA(startedAt time.Time, done, total int64) string {
	return formatETAAt(time.Now(), startedAt, done, total)
}

// FormatETAAt exposes the now-parameterised ETA helper for golden
// tests that need a fixed clock.
func FormatETAAt(now, startedAt time.Time, done, total int64) string {
	return formatETAAt(now, startedAt, done, total)
}

// ProgressPct exposes progressPct for tests.
func ProgressPct(done, total int64) int { return progressPct(done, total) }

// HxValsJSON exposes hxValsJSON for tests.
func HxValsJSON(pairs ...any) (string, error) { return hxValsJSON(pairs...) }

// BrowseURL exposes browseURL for tests.
func BrowseURL(pairs ...any) (string, error) { return browseURL(pairs...) }
