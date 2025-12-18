// Package triebuild implements streaming trie construction from sorted keys.
package triebuild

// Node represents a prefix node in the trie.
type Node struct {
	Prefix           string
	Pos              uint64
	Depth            uint32
	SubtreeEnd       uint64
	ObjectCount      uint64
	TotalBytes       uint64
	MaxDepthInSubtree uint32
}

// Builder constructs a trie from a sorted stream of keys.
type Builder struct {
	// Will be implemented in subsequent commits
}
