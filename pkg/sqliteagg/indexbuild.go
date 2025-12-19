package sqliteagg

import (
	"fmt"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/triebuild"
)

// BuildTrieFromSQLite builds a trie from the pre-aggregated prefix stats in SQLite.
// The prefixes must be in lexicographic order (which SQLite provides via ORDER BY).
func BuildTrieFromSQLite(agg *Aggregator) (*triebuild.Result, error) {
	log := logging.WithPhase("build_trie")

	prefixCount, err := agg.PrefixCount()
	if err != nil {
		return nil, fmt.Errorf("get prefix count: %w", err)
	}

	maxDepth, err := agg.MaxDepth()
	if err != nil {
		return nil, fmt.Errorf("get max depth: %w", err)
	}

	presentTiers, err := agg.PresentTiers()
	if err != nil {
		return nil, fmt.Errorf("get present tiers: %w", err)
	}

	log.Info().
		Uint64("prefix_count", prefixCount).
		Uint32("max_depth", maxDepth).
		Int("present_tiers", len(presentTiers)).
		Msg("building trie from SQLite")

	iter, err := agg.IteratePrefixes()
	if err != nil {
		return nil, fmt.Errorf("iterate prefixes: %w", err)
	}
	defer iter.Close()

	builder := newTrieBuilder(prefixCount, len(presentTiers) > 0)
	startTime := time.Now()
	lastLogTime := time.Now()
	processedCount := uint64(0)

	for iter.Next() {
		row := iter.Row()

		builder.addPrefix(row)

		processedCount++

		// Log progress every 5 seconds
		if time.Since(lastLogTime) >= 5*time.Second {
			log.Info().
				Uint64("prefixes_processed", processedCount).
				Uint64("prefixes_total", prefixCount).
				Dur("elapsed", time.Since(startTime)).
				Msg("trie build progress")
			lastLogTime = time.Now()
		}
	}

	if iter.Err() != nil {
		return nil, fmt.Errorf("iterate prefixes: %w", iter.Err())
	}

	result := builder.finish()
	result.MaxDepth = maxDepth
	result.TrackTiers = len(presentTiers) > 0
	result.PresentTiers = presentTiers

	elapsed := time.Since(startTime)
	nodeCount := int64(len(result.Nodes))
	event := log.Info().
		Int("total_nodes", int(nodeCount)).
		Uint32("max_depth", result.MaxDepth).
		Int("present_tiers", len(result.PresentTiers)).
		Dur("elapsed", elapsed)
	if logging.IsPrettyMode() {
		event = event.
			Str("total_nodes_h", humanfmt.Count(nodeCount)).
			Str("elapsed_h", humanfmt.Duration(elapsed))
	}
	event.Msg("trie build complete")

	return result, nil
}

// trieBuilder builds a trie from pre-aggregated prefix stats.
type trieBuilder struct {
	nodes      []triebuild.Node
	stack      []stackEntry
	posCount   uint64
	trackTiers bool
}

type stackEntry struct {
	prefix            string
	pos               uint64
	depth             int
	maxDepthInSubtree uint32
}

func newTrieBuilder(estimatedSize uint64, trackTiers bool) *trieBuilder {
	return &trieBuilder{
		nodes:      make([]triebuild.Node, 0, estimatedSize),
		stack:      make([]stackEntry, 0, 32),
		trackTiers: trackTiers,
	}
}

func (b *trieBuilder) addPrefix(row PrefixRow) {
	// Close nodes that are no longer ancestors of this prefix
	b.closeNodesNotAncestorOf(row.Prefix)

	// Open new node for this prefix
	pos := b.posCount
	b.posCount++

	entry := stackEntry{
		prefix:            row.Prefix,
		pos:               pos,
		depth:             row.Depth,
		maxDepthInSubtree: uint32(row.Depth),
	}
	b.stack = append(b.stack, entry)

	// Create node with stats from SQLite
	node := triebuild.Node{
		Prefix:            row.Prefix,
		Pos:               pos,
		Depth:             uint32(row.Depth),
		ObjectCount:       row.TotalCount,
		TotalBytes:        row.TotalBytes,
		MaxDepthInSubtree: uint32(row.Depth), // Will be updated when children are closed
		// SubtreeEnd will be set when this node is closed
	}

	// Copy tier stats
	if b.trackTiers {
		node.TierCounts = row.TierCounts
		node.TierBytes = row.TierBytes
	}

	b.storeNode(node)
}

func (b *trieBuilder) closeNodesNotAncestorOf(prefix string) {
	for len(b.stack) > 0 {
		top := b.stack[len(b.stack)-1]

		// Check if top is an ancestor of prefix
		if isAncestor(top.prefix, prefix) {
			break
		}

		// Close this node
		b.closeTopNode()
	}
}

func (b *trieBuilder) closeTopNode() {
	if len(b.stack) == 0 {
		return
	}

	top := b.stack[len(b.stack)-1]
	b.stack = b.stack[:len(b.stack)-1]

	// subtree_end is the last assigned pos (posCount - 1)
	subtreeEnd := b.posCount - 1

	// Update the node
	b.nodes[top.pos].SubtreeEnd = subtreeEnd
	b.nodes[top.pos].MaxDepthInSubtree = top.maxDepthInSubtree

	// Propagate max depth to parent
	if len(b.stack) > 0 {
		parent := &b.stack[len(b.stack)-1]
		if top.maxDepthInSubtree > parent.maxDepthInSubtree {
			parent.maxDepthInSubtree = top.maxDepthInSubtree
		}
	}
}

func (b *trieBuilder) storeNode(node triebuild.Node) {
	if uint64(len(b.nodes)) <= node.Pos {
		newSize := node.Pos + 1
		if newSize < uint64(len(b.nodes))*2 {
			newSize = uint64(len(b.nodes)) * 2
		}
		if newSize < 64 {
			newSize = 64
		}
		newNodes := make([]triebuild.Node, newSize)
		copy(newNodes, b.nodes)
		b.nodes = newNodes
	}
	b.nodes[node.Pos] = node
}

func (b *trieBuilder) finish() *triebuild.Result {
	// Close all remaining nodes
	for len(b.stack) > 0 {
		b.closeTopNode()
	}

	// Trim nodes slice to actual size
	b.nodes = b.nodes[:b.posCount]

	return &triebuild.Result{
		Nodes: b.nodes,
	}
}

// isAncestor returns true if ancestor is a prefix ancestor of descendant.
// The root (empty string) is an ancestor of everything.
// "a/" is an ancestor of "a/b/" and "a/b/c/".
func isAncestor(ancestor, descendant string) bool {
	if ancestor == "" {
		return true
	}
	if len(ancestor) >= len(descendant) {
		return false
	}
	return descendant[:len(ancestor)] == ancestor
}
