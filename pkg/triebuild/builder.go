// Package triebuild implements streaming trie construction from sorted keys.
package triebuild

import (
	"strings"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
)

// Node represents a prefix node in the trie.
type Node struct {
	Prefix            string
	Pos               uint64
	Depth             uint32
	SubtreeEnd        uint64
	ObjectCount       uint64
	TotalBytes        uint64
	MaxDepthInSubtree uint32
}

// Result holds the complete trie build output.
type Result struct {
	Nodes    []Node
	MaxDepth uint32
}

// stackNode is an in-progress node on the build stack.
type stackNode struct {
	prefix            string
	pos               uint64
	depth             uint32
	objectCount       uint64
	totalBytes        uint64
	maxDepthInSubtree uint32
}

// Builder constructs a trie from a sorted stream of keys.
type Builder struct {
	stack     []stackNode
	nodes     []Node
	posCount  uint64
	maxDepth  uint32
}

// New creates a new trie builder.
func New() *Builder {
	return &Builder{}
}

// Build processes a sorted iterator and returns the trie structure.
func (b *Builder) Build(iter extsort.Iterator) (*Result, error) {
	// Initialize root node
	b.openNode("", 0)

	for iter.Next() {
		rec := iter.Record()
		if err := b.processKey(rec.Key, rec.Size); err != nil {
			return nil, err
		}
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	// Close all remaining nodes
	b.closeAll()

	return &Result{
		Nodes:    b.nodes,
		MaxDepth: b.maxDepth,
	}, nil
}

// processKey handles a single object key.
func (b *Builder) processKey(key string, size uint64) error {
	// Extract prefix chain for this key
	prefixes := extractPrefixes(key)

	// Find LCP with current stack
	lcpDepth := b.findLCPDepth(prefixes)

	// Close nodes above LCP
	b.closeNodesAbove(lcpDepth)

	// Open new nodes for remaining prefixes
	for i := lcpDepth; i < len(prefixes); i++ {
		b.openNode(prefixes[i], uint32(i+1))
	}

	// Aggregate stats into all ancestor nodes (including root)
	for i := range b.stack {
		b.stack[i].objectCount++
		b.stack[i].totalBytes += size
	}

	return nil
}

// extractPrefixes returns all directory prefixes for a key.
// For "a/b/c.txt", returns ["a/", "a/b/"]
// For "a/b/c/", returns ["a/", "a/b/", "a/b/c/"]
func extractPrefixes(key string) []string {
	var prefixes []string
	start := 0

	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			prefix := key[:i+1]
			prefixes = append(prefixes, prefix)
			start = i + 1
		}
	}

	// Handle keys that end with / (folder markers)
	// The last component is already included

	_ = start
	return prefixes
}

// findLCPDepth finds the longest common prefix depth between
// the given prefixes and the current stack.
func (b *Builder) findLCPDepth(prefixes []string) int {
	// Stack has root at index 0 (depth 0), then prefix nodes
	// Stack depth corresponds to prefix index + 1
	lcpDepth := 0

	for i := 0; i < len(prefixes) && i+1 < len(b.stack); i++ {
		if b.stack[i+1].prefix == prefixes[i] {
			lcpDepth = i + 1
		} else {
			break
		}
	}

	return lcpDepth
}

// closeNodesAbove closes all nodes with depth > targetDepth.
func (b *Builder) closeNodesAbove(targetDepth int) {
	for len(b.stack) > targetDepth+1 {
		b.closeTopNode()
	}
}

// closeTopNode closes the node at the top of the stack.
func (b *Builder) closeTopNode() {
	if len(b.stack) == 0 {
		return
	}

	top := b.stack[len(b.stack)-1]
	b.stack = b.stack[:len(b.stack)-1]

	// subtree_end is the last assigned pos (posCount - 1)
	subtreeEnd := b.posCount - 1

	node := Node{
		Prefix:            top.prefix,
		Pos:               top.pos,
		Depth:             top.depth,
		SubtreeEnd:        subtreeEnd,
		ObjectCount:       top.objectCount,
		TotalBytes:        top.totalBytes,
		MaxDepthInSubtree: top.maxDepthInSubtree,
	}

	// Store node at its position
	b.storeNode(node)

	// Propagate max depth to parent
	if len(b.stack) > 0 {
		if top.maxDepthInSubtree > b.stack[len(b.stack)-1].maxDepthInSubtree {
			b.stack[len(b.stack)-1].maxDepthInSubtree = top.maxDepthInSubtree
		}
	}
}

// openNode creates a new node and pushes it onto the stack.
func (b *Builder) openNode(prefix string, depth uint32) {
	pos := b.posCount
	b.posCount++

	sn := stackNode{
		prefix:            prefix,
		pos:               pos,
		depth:             depth,
		objectCount:       0,
		totalBytes:        0,
		maxDepthInSubtree: depth,
	}

	b.stack = append(b.stack, sn)

	// Track global max depth
	if depth > b.maxDepth {
		b.maxDepth = depth
	}
}

// storeNode stores a finalized node.
func (b *Builder) storeNode(node Node) {
	// Ensure slice is large enough
	if uint64(len(b.nodes)) <= node.Pos {
		newSize := node.Pos + 1
		if newSize < uint64(len(b.nodes))*2 {
			newSize = uint64(len(b.nodes)) * 2
		}
		if newSize < 64 {
			newSize = 64
		}
		newNodes := make([]Node, newSize)
		copy(newNodes, b.nodes)
		b.nodes = newNodes
	}
	b.nodes[node.Pos] = node
}

// closeAll closes all remaining nodes on the stack.
func (b *Builder) closeAll() {
	for len(b.stack) > 0 {
		b.closeTopNode()
	}
	// Trim nodes slice to actual size
	b.nodes = b.nodes[:b.posCount]
}

// PrefixStrings returns all prefix strings from the result in position order.
func (r *Result) PrefixStrings() []string {
	prefixes := make([]string, len(r.Nodes))
	for i, n := range r.Nodes {
		prefixes[i] = n.Prefix
	}
	return prefixes
}

// VerifySubtreeRanges validates that subtree ranges are correct.
func (r *Result) VerifySubtreeRanges() bool {
	for i, node := range r.Nodes {
		pos := uint64(i)
		if node.Pos != pos {
			return false
		}
		if node.SubtreeEnd < pos {
			return false
		}
		if node.SubtreeEnd >= uint64(len(r.Nodes)) {
			return false
		}
	}
	return true
}

// VerifyDepthOrder validates that depths are consistent.
func (r *Result) VerifyDepthOrder() bool {
	// Root should have depth 0
	if len(r.Nodes) > 0 && r.Nodes[0].Depth != 0 {
		return false
	}
	return true
}

// BuildFromKeys is a convenience function that builds a trie from
// a slice of keys (for testing).
func BuildFromKeys(keys []string, sizes []uint64) (*Result, error) {
	b := New()

	// Initialize root
	b.openNode("", 0)

	for i, key := range keys {
		size := uint64(0)
		if i < len(sizes) {
			size = sizes[i]
		}
		if err := b.processKey(key, size); err != nil {
			return nil, err
		}
	}

	b.closeAll()

	return &Result{
		Nodes:    b.nodes,
		MaxDepth: b.maxDepth,
	}, nil
}

// GetNodeByPrefix finds a node by its prefix string.
func (r *Result) GetNodeByPrefix(prefix string) (Node, bool) {
	for _, n := range r.Nodes {
		if n.Prefix == prefix {
			return n, true
		}
	}
	return Node{}, false
}

// GetDescendants returns all nodes that are descendants of the given position.
func (r *Result) GetDescendants(pos uint64) []Node {
	if pos >= uint64(len(r.Nodes)) {
		return nil
	}

	node := r.Nodes[pos]
	var descendants []Node

	for i := pos + 1; i <= node.SubtreeEnd; i++ {
		descendants = append(descendants, r.Nodes[i])
	}

	return descendants
}

// CountPrefix counts how many prefix strings start with the given prefix.
func countPrefix(prefixes []string, prefix string) int {
	count := 0
	for _, p := range prefixes {
		if strings.HasPrefix(p, prefix) {
			count++
		}
	}
	return count
}
