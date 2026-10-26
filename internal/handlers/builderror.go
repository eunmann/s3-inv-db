package handlers

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// s3URIPattern finds an s3:// URI inside an error chain; the trailing
// ": " of the next wrapper is trimmed off by s3URIIn.
var s3URIPattern = regexp.MustCompile(`s3://\S+`)

// maxErrorSummaryRunes caps the fallback summary: a wrapped error's
// innermost message is short, but a library can put a whole response
// body there.
const maxErrorSummaryRunes = 200

// summarizeBuildError turns a build's error chain into a sentence a
// user can act on. The chain reads outermost wrapper first, so the
// cause is at its end; the raw text stays in the drawer's technical
// details.
func summarizeBuildError(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	lower := strings.ToLower(raw)
	switch {
	case strings.Contains(lower, "context canceled"):
		return "Cancelled."
	case strings.Contains(lower, "server restart"), strings.Contains(lower, "server shut down"):
		return "Interrupted by a server restart. Load it again to rebuild it."
	case containsAny(lower, "nosuchkey", "statuscode: 404"):
		if uri := s3URIIn(raw); uri != "" {
			return "An inventory data file is missing from S3: " + uri + ". The inventory may have been deleted or moved."
		}

		return "An inventory data file is missing from S3. The inventory may have been deleted or moved."
	case containsAny(lower, "accessdenied", "statuscode: 403"):
		if uri := s3URIIn(raw); uri != "" {
			return "Access denied reading " + uri + ". Check the server's S3 permissions."
		}

		return "Access denied reading from S3. Check the server's S3 permissions."
	case containsAny(lower, "disk-budget", "disk budget"):
		return "Not enough room in the index disk budget. Unload or unpin a run, or raise --max-index-disk."
	case containsAny(lower, "no space left", "enospc"):
		return "The server ran out of disk space while building the index."
	case strings.Contains(lower, "checksum"):
		return "A file failed its checksum check and may be corrupt. Load it again to rebuild it."
	case strings.Contains(lower, "manifest"):
		return "The inventory's manifest could not be used: " + strings.TrimRight(innermostError(raw), ".") + "."
	}

	return sentence(innermostError(raw))
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}

	return false
}

func s3URIIn(raw string) string {
	return strings.TrimRight(s3URIPattern.FindString(raw), ":,;")
}

// innermostError is the text after the last ": " of a wrapped error
// chain: the cause, without the wrappers' context.
func innermostError(raw string) string {
	if i := strings.LastIndex(raw, ": "); i >= 0 && i+2 < len(raw) {
		return strings.TrimSpace(raw[i+2:])
	}

	return raw
}

// sentence capitalizes s, caps its length and ends it with a period.
func sentence(s string) string {
	if utf8.RuneCountInString(s) > maxErrorSummaryRunes {
		s = string([]rune(s)[:maxErrorSummaryRunes]) + "…"
	}
	r, size := utf8.DecodeRuneInString(s)
	s = string(unicode.ToUpper(r)) + s[size:]
	if !strings.HasSuffix(s, ".") && !strings.HasSuffix(s, "!") && !strings.HasSuffix(s, "?") && !strings.HasSuffix(s, "…") {
		s += "."
	}

	return s
}
