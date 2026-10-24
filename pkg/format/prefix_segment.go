package format

// ChildSegment returns the name a listing shows for the prefix at pos:
// its last segment before the trailing slash ("2024" for "data/2024/"),
// exactly what trimming the parent and the slash off the full prefix
// gives. It reads the preloaded segment cache and does not allocate.
// The root and out-of-range positions return "".
func (r *DictPrefixReader) ChildSegment(pos uint64) string {
	if pos >= r.Count() {
		return ""
	}
	start := r.offsets.UnsafeGetU64(pos)
	end := r.offsets.UnsafeGetU64(pos + 1)
	// A stored prefix "a/b/" splits into ["a", "b", ""]: the trailing
	// empty segment is the slash, the one before it is the name.
	if end-start < 2 {
		return ""
	}

	return r.cache.Get(r.readSegID(end - 2))
}

// ChildSegment is DictPrefixReader.ChildSegment for this index.
func (m *MPHF) ChildSegment(pos uint64) string {
	if m.dictPrefixes == nil {
		return ""
	}

	return m.dictPrefixes.ChildSegment(pos)
}
