package handlers

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
)

// parseFilter reads min_count and min_bytes from q. Missing keys are
// zero. Returns an error naming the offending key on invalid input;
// callers map this to a 400.
func parseFilter(q url.Values) (indexread.Filter, error) {
	var f indexread.Filter
	if v := q.Get("min_count"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return f, fmt.Errorf("invalid min_count")
		}
		f.MinCount = n
	}
	if v := q.Get("min_bytes"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return f, fmt.Errorf("invalid min_bytes")
		}
		f.MinBytes = n
	}

	return f, nil
}

// parsePositiveInt reads key from q as an int >= 1. Returns (def, nil)
// when the key is absent. Returns an error message suitable for a 400
// when the value is present but malformed or below 1.
func parsePositiveInt(q url.Values, key string, def int) (int, error) {
	v := q.Get(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("invalid %s", key)
	}

	return n, nil
}

// parseCompareOpts parses the shared compare query string for both
// ComparePage and CompareLevelAPI.
func parseCompareOpts(q url.Values) compareViewOptions {
	pageParams := inventory.NormalizePage(q.Get("page"), q.Get("page_size"))
	sortParams := inventory.NormalizeCompareSort(q.Get("sort"), q.Get("dir"))

	return compareViewOptions{
		from:          inventory.ID(q.Get("from")),
		to:            inventory.ID(q.Get("to")),
		prefix:        q.Get("prefix"),
		hideUnchanged: q.Get("show_unchanged") != trueLiteral,
		page:          pageParams.Page,
		pageSize:      pageParams.Size,
		sortBy:        sortParams.Col,
		dir:           sortParams.Dir,
	}
}
