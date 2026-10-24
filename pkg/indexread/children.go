package indexread

// ChildSegment returns the name a listing shows for the prefix at pos —
// its last segment, without the parent or the trailing slash — without
// allocating. Sorting children by name through it avoids building every
// child's full prefix string. The root returns "".
func (idx *Index) ChildSegment(pos uint64) string {
	return idx.mphf.ChildSegment(pos)
}
