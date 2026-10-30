package indexread

import "github.com/eunmann/s3-inv-db/pkg/format"

// ChildSegment returns the name a listing shows for the prefix at pos —
// its last segment, without the parent or the trailing slash — as a
// copy that stays valid after Close. The root returns "".
func (idx *Index) ChildSegment(pos uint64) string {
	return idx.prefixes.ChildSegment(pos)
}

// CompareChildNames compares the name of a's prefix at pa with the name
// of b's at pb in the order siblings are stored, name then slash
// ("2024-x" before "2024"), without allocating: merging two indexes'
// children walks both in that order.
func CompareChildNames(a *Index, pa uint64, b *Index, pb uint64) int {
	return format.CompareChildSegments(a.prefixes, pa, b.prefixes, pb)
}

// ChildPrefix returns the prefix at pos and its name (ChildSegment) in
// one allocation, the prefix built as parent, the prefix of pos's
// parent, then the name and a slash. A listing row needs both.
//
//nolint:gocritic // gocritic wants named returns, nonamedreturns forbids them
func (idx *Index) ChildPrefix(parent string, pos uint64) (string, string) {
	return idx.prefixes.ChildPrefix(parent, pos)
}
