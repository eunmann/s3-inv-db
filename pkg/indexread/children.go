package indexread

// ChildSegment returns the name a listing shows for the prefix at pos —
// its last segment, without the parent or the trailing slash — as a
// copy that stays valid after Close. The root returns "".
func (idx *Index) ChildSegment(pos uint64) string {
	return idx.prefixes.ChildSegment(pos)
}

// AppendChildSegment appends ChildSegment(pos) to dst. Comparing or
// ordering children by name through one reused buffer costs no
// allocation per child and builds no full prefix string.
func (idx *Index) AppendChildSegment(dst []byte, pos uint64) []byte {
	return idx.prefixes.AppendChildSegment(dst, pos)
}
