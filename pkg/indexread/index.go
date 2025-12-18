// Package indexread provides read-only access to S3 inventory indexes.
package indexread

// Index provides low-latency access to an S3 inventory index via mmap.
type Index struct {
	// Will be implemented in subsequent commits
}

// Stats holds aggregated statistics for a prefix.
type Stats struct {
	ObjectCount uint64
	TotalBytes  uint64
}

// Lookup returns the preorder position for a prefix, or ok=false if not found.
func (idx *Index) Lookup(prefix string) (pos uint64, ok bool) {
	// Will be implemented after MPHF integration
	return 0, false
}

// Stats returns the object count and total bytes for the node at pos.
func (idx *Index) Stats(pos uint64) Stats {
	// Will be implemented after columnar format is done
	return Stats{}
}

// DescendantsAtDepth returns an iterator over positions of descendants
// at exactly the given relative depth, in alphabetical order.
func (idx *Index) DescendantsAtDepth(prefixPos uint64, relDepth int) Iterator {
	// Will be implemented after depth posting lists
	return nil
}

// DescendantsUpToDepth returns an iterator over positions of descendants
// up to the given relative depth, grouped by depth then alphabetical.
func (idx *Index) DescendantsUpToDepth(prefixPos uint64, maxRelDepth int) Iterator {
	// Will be implemented after depth posting lists
	return nil
}

// PrefixString returns the prefix string for the node at pos.
func (idx *Index) PrefixString(pos uint64) string {
	// Will be implemented after prefix blob is done
	return ""
}

// Iterator provides sequential access to node positions.
type Iterator interface {
	// Next advances to the next position. Returns false when done.
	Next() bool
	// Pos returns the current position. Valid after Next() returns true.
	Pos() uint64
	// Depth returns the depth of the current position.
	Depth() uint32
}
