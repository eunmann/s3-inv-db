package templates

import "time"

// FormatETA exposes the time-bound ETA helper for tests, using
// time.Now so existing tests don't have to pass a clock.
func FormatETA(startedAt time.Time, done, total int64) string {
	return formatETAAt(time.Now(), startedAt, done, total)
}

// LiveSegments exposes liveSegments for tests.
func LiveSegments(state, stage, step string, done, total int64) []LiveSegment {
	return liveSegments(state, stage, step, done, total)
}

// ProgressPct exposes progressPct for tests.
func ProgressPct(done, total int64) int { return progressPct(done, total) }

// HxValsJSON exposes hxValsJSON for tests.
func HxValsJSON(pairs ...any) (string, error) { return hxValsJSON(pairs...) }

// HxValsJSONGeneral exposes the general encoder hxValsJSON's fast path
// must match byte for byte.
func HxValsJSONGeneral(pairs ...any) (string, error) { return marshalHxVals(pairs) }
