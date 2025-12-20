package extsort

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/tiers"
)

// IndexBuilder builds index files directly from a sorted stream of PrefixRows.
// It processes prefixes in a single streaming pass, computing preorder positions
// and subtree ranges on the fly without building an in-memory trie.
//
// The builder uses a depth stack to track ancestors and close subtrees as
// prefixes are processed in sorted (preorder) order.
type IndexBuilder struct {
	outDir string

	// Writers for columnar arrays
	objectCountW *format.ArrayWriter
	totalBytesW  *format.ArrayWriter
	depthW       *format.ArrayWriter

	// Tier stats writers (one pair per present tier)
	tierCountWriters map[tiers.ID]*format.ArrayWriter
	tierBytesWriters map[tiers.ID]*format.ArrayWriter

	// MPHF builder (collects all prefixes, built at end)
	mphfBuilder *format.MPHFBuilder

	// Depth index builder
	depthIndexBuilder *format.DepthIndexBuilder

	// Stack for computing subtree ranges
	stack []stackEntry

	// Current position counter
	posCount uint64

	// Track max depth
	maxDepth uint32

	// Track which tiers have data
	presentTiers map[tiers.ID]bool

	// Buffered subtree_end values (written at end after finalization)
	subtreeEnds []uint64

	// Buffered max_depth_in_subtree values
	maxDepthInSubtrees []uint32

	closed bool
}

// stackEntry tracks an open prefix on the stack.
type stackEntry struct {
	prefix            string
	pos               uint64
	depth             uint32
	maxDepthInSubtree uint32
}

// NewIndexBuilder creates a streaming index builder.
func NewIndexBuilder(outDir string) (*IndexBuilder, error) {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return nil, fmt.Errorf("create output dir: %w", err)
	}

	b := &IndexBuilder{
		outDir:             outDir,
		mphfBuilder:        format.NewMPHFBuilder(),
		depthIndexBuilder:  format.NewDepthIndexBuilder(),
		stack:              make([]stackEntry, 0, 32),
		presentTiers:       make(map[tiers.ID]bool),
		subtreeEnds:        make([]uint64, 0, 1024),
		maxDepthInSubtrees: make([]uint32, 0, 1024),
		tierCountWriters:   make(map[tiers.ID]*format.ArrayWriter),
		tierBytesWriters:   make(map[tiers.ID]*format.ArrayWriter),
	}

	var err error

	// Open columnar array writers
	b.objectCountW, err = format.NewArrayWriter(filepath.Join(outDir, "object_count.u64"), 8)
	if err != nil {
		return nil, fmt.Errorf("create object_count writer: %w", err)
	}

	b.totalBytesW, err = format.NewArrayWriter(filepath.Join(outDir, "total_bytes.u64"), 8)
	if err != nil {
		b.cleanup()
		return nil, fmt.Errorf("create total_bytes writer: %w", err)
	}

	b.depthW, err = format.NewArrayWriter(filepath.Join(outDir, "depth.u32"), 4)
	if err != nil {
		b.cleanup()
		return nil, fmt.Errorf("create depth writer: %w", err)
	}

	// subtreeEnd and maxDepthInSubtree are written at the end (need finalization)
	// We'll create their writers during Finalize()

	return b, nil
}

// cleanup closes and removes all partially created files on error.
func (b *IndexBuilder) cleanup() {
	if b.objectCountW != nil {
		b.objectCountW.Close()
	}
	if b.totalBytesW != nil {
		b.totalBytesW.Close()
	}
	if b.depthW != nil {
		b.depthW.Close()
	}
	for _, w := range b.tierCountWriters {
		w.Close()
	}
	for _, w := range b.tierBytesWriters {
		w.Close()
	}
	os.RemoveAll(b.outDir)
}

// Add processes a single PrefixRow from the sorted stream.
// Prefixes must be added in lexicographic (sorted) order.
func (b *IndexBuilder) Add(row *PrefixRow) error {
	// Find the common ancestor with the current stack
	commonDepth := b.findCommonAncestorDepth(row.Prefix, int(row.Depth))

	// Close nodes above common ancestor
	if err := b.closeNodesAbove(commonDepth); err != nil {
		return err
	}

	// Assign position
	pos := b.posCount
	b.posCount++

	// Push new node onto stack
	b.stack = append(b.stack, stackEntry{
		prefix:            row.Prefix,
		pos:               pos,
		depth:             uint32(row.Depth),
		maxDepthInSubtree: uint32(row.Depth),
	})

	// Track max depth
	if uint32(row.Depth) > b.maxDepth {
		b.maxDepth = uint32(row.Depth)
	}

	// Add to depth index builder
	b.depthIndexBuilder.Add(pos, uint32(row.Depth))

	// Add to MPHF builder
	b.mphfBuilder.Add(row.Prefix, pos)

	// Write stats to columnar arrays
	if err := b.objectCountW.WriteU64(row.Count); err != nil {
		return fmt.Errorf("write object_count: %w", err)
	}
	if err := b.totalBytesW.WriteU64(row.TotalBytes); err != nil {
		return fmt.Errorf("write total_bytes: %w", err)
	}
	if err := b.depthW.WriteU32(uint32(row.Depth)); err != nil {
		return fmt.Errorf("write depth: %w", err)
	}

	// Reserve space for subtreeEnd and maxDepthInSubtree (filled during finalize)
	b.subtreeEnds = append(b.subtreeEnds, 0)
	b.maxDepthInSubtrees = append(b.maxDepthInSubtrees, 0)

	// Track tier data
	for tierID := tiers.ID(0); tierID < tiers.NumTiers; tierID++ {
		if row.TierCounts[tierID] > 0 || row.TierBytes[tierID] > 0 {
			b.presentTiers[tierID] = true
		}
	}

	// Write tier stats (lazy create writers)
	if err := b.writeTierStats(row); err != nil {
		return err
	}

	return nil
}

// findCommonAncestorDepth finds the depth of the deepest common ancestor.
func (b *IndexBuilder) findCommonAncestorDepth(prefix string, depth int) int {
	commonDepth := 0

	for i := 0; i < len(b.stack); i++ {
		entry := &b.stack[i]
		// Check if stack entry is a prefix of the new prefix
		if len(entry.prefix) <= len(prefix) && prefix[:len(entry.prefix)] == entry.prefix {
			commonDepth = i + 1
		} else {
			break
		}
	}

	return commonDepth
}

// closeNodesAbove closes all stack nodes with depth > targetDepth.
func (b *IndexBuilder) closeNodesAbove(targetDepth int) error {
	for len(b.stack) > targetDepth {
		if err := b.closeTopNode(); err != nil {
			return err
		}
	}
	return nil
}

// closeTopNode closes the node at the top of the stack.
func (b *IndexBuilder) closeTopNode() error {
	if len(b.stack) == 0 {
		return nil
	}

	top := b.stack[len(b.stack)-1]
	b.stack = b.stack[:len(b.stack)-1]

	// subtree_end is the last assigned pos (posCount - 1)
	subtreeEnd := b.posCount - 1

	// Store subtree_end and maxDepthInSubtree
	b.subtreeEnds[top.pos] = subtreeEnd
	b.maxDepthInSubtrees[top.pos] = top.maxDepthInSubtree

	// Propagate max depth to parent
	if len(b.stack) > 0 {
		if top.maxDepthInSubtree > b.stack[len(b.stack)-1].maxDepthInSubtree {
			b.stack[len(b.stack)-1].maxDepthInSubtree = top.maxDepthInSubtree
		}
	}

	return nil
}

// writeTierStats writes tier statistics for a row.
func (b *IndexBuilder) writeTierStats(row *PrefixRow) error {
	for tierID := tiers.ID(0); tierID < tiers.NumTiers; tierID++ {
		// Check if we need to create a writer for this tier
		_, hasCountWriter := b.tierCountWriters[tierID]
		_, hasBytesWriter := b.tierBytesWriters[tierID]

		if !hasCountWriter || !hasBytesWriter {
			// No writers yet - check if this row has data for this tier
			if row.TierCounts[tierID] == 0 && row.TierBytes[tierID] == 0 {
				continue
			}
			// First data for this tier - need to create writers and backfill zeros
			if err := b.createTierWriter(tierID, row); err != nil {
				return err
			}
		}

		// Write tier data if writer exists
		if countW, ok := b.tierCountWriters[tierID]; ok {
			if err := countW.WriteU64(row.TierCounts[tierID]); err != nil {
				return fmt.Errorf("write tier %d count: %w", tierID, err)
			}
		}
		if bytesW, ok := b.tierBytesWriters[tierID]; ok {
			if err := bytesW.WriteU64(row.TierBytes[tierID]); err != nil {
				return fmt.Errorf("write tier %d bytes: %w", tierID, err)
			}
		}
	}
	return nil
}

// createTierWriter creates writers for a tier and backfills zeros for previous positions.
func (b *IndexBuilder) createTierWriter(tierID tiers.ID, row *PrefixRow) error {
	// Create tier_stats directory if needed
	tierDir := filepath.Join(b.outDir, "tier_stats")
	if err := os.MkdirAll(tierDir, 0755); err != nil {
		return fmt.Errorf("create tier_stats dir: %w", err)
	}

	// Get tier info for file naming
	mapping := tiers.NewMapping()
	info := mapping.ByID(tierID)

	// Create count writer using tier name
	countPath := filepath.Join(tierDir, info.FilePrefix+"_count.u64")
	countW, err := format.NewArrayWriter(countPath, 8)
	if err != nil {
		return fmt.Errorf("create tier %s count writer: %w", info.Name, err)
	}
	b.tierCountWriters[tierID] = countW

	// Create bytes writer using tier name
	bytesPath := filepath.Join(tierDir, info.FilePrefix+"_bytes.u64")
	bytesW, err := format.NewArrayWriter(bytesPath, 8)
	if err != nil {
		countW.Close()
		return fmt.Errorf("create tier %s bytes writer: %w", info.Name, err)
	}
	b.tierBytesWriters[tierID] = bytesW

	// Backfill zeros for all previous positions
	for i := uint64(0); i < b.posCount-1; i++ {
		if err := countW.WriteU64(0); err != nil {
			return fmt.Errorf("backfill tier %s count: %w", info.Name, err)
		}
		if err := bytesW.WriteU64(0); err != nil {
			return fmt.Errorf("backfill tier %s bytes: %w", info.Name, err)
		}
	}

	return nil
}

// AddAll processes all PrefixRows from an iterator.
func (b *IndexBuilder) AddAll(iter *MergeIterator) error {
	for {
		row, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read from iterator: %w", err)
		}
		if err := b.Add(row); err != nil {
			return fmt.Errorf("add row: %w", err)
		}
	}
	return nil
}

// Finalize closes all remaining stack nodes and writes final files.
func (b *IndexBuilder) Finalize() error {
	if b.closed {
		return nil
	}
	b.closed = true

	// Close all remaining nodes on stack
	if err := b.closeNodesAbove(0); err != nil {
		return fmt.Errorf("close remaining nodes: %w", err)
	}

	// Close columnar array writers for stats
	if err := b.objectCountW.Close(); err != nil {
		return fmt.Errorf("close object_count: %w", err)
	}
	if err := b.totalBytesW.Close(); err != nil {
		return fmt.Errorf("close total_bytes: %w", err)
	}
	if err := b.depthW.Close(); err != nil {
		return fmt.Errorf("close depth: %w", err)
	}

	// Close tier writers
	for tierID, w := range b.tierCountWriters {
		if err := w.Close(); err != nil {
			return fmt.Errorf("close tier %d count: %w", tierID, err)
		}
	}
	for tierID, w := range b.tierBytesWriters {
		if err := w.Close(); err != nil {
			return fmt.Errorf("close tier %d bytes: %w", tierID, err)
		}
	}

	// Write subtree_end array
	subtreeEndW, err := format.NewArrayWriter(filepath.Join(b.outDir, "subtree_end.u64"), 8)
	if err != nil {
		return fmt.Errorf("create subtree_end writer: %w", err)
	}
	for _, v := range b.subtreeEnds {
		if err := subtreeEndW.WriteU64(v); err != nil {
			subtreeEndW.Close()
			return fmt.Errorf("write subtree_end: %w", err)
		}
	}
	if err := subtreeEndW.Close(); err != nil {
		return fmt.Errorf("close subtree_end: %w", err)
	}

	// Write max_depth_in_subtree array
	maxDepthW, err := format.NewArrayWriter(filepath.Join(b.outDir, "max_depth_in_subtree.u32"), 4)
	if err != nil {
		return fmt.Errorf("create max_depth_in_subtree writer: %w", err)
	}
	for _, v := range b.maxDepthInSubtrees {
		if err := maxDepthW.WriteU32(v); err != nil {
			maxDepthW.Close()
			return fmt.Errorf("write max_depth_in_subtree: %w", err)
		}
	}
	if err := maxDepthW.Close(); err != nil {
		return fmt.Errorf("close max_depth_in_subtree: %w", err)
	}

	// Build depth index
	if err := b.depthIndexBuilder.Build(b.outDir); err != nil {
		return fmt.Errorf("build depth index: %w", err)
	}

	// Build MPHF
	if err := b.mphfBuilder.Build(b.outDir); err != nil {
		return fmt.Errorf("build MPHF: %w", err)
	}

	// Write tier manifest if we have tier data
	if len(b.presentTiers) > 0 {
		presentTierList := make([]tiers.ID, 0, len(b.presentTiers))
		for tierID := range b.presentTiers {
			presentTierList = append(presentTierList, tierID)
		}
		if err := tiers.WriteManifest(b.outDir, presentTierList); err != nil {
			return fmt.Errorf("write tier manifest: %w", err)
		}
	}

	// Write manifest
	if err := format.WriteManifest(b.outDir, b.posCount, b.maxDepth); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Sync directory
	if err := format.SyncDir(b.outDir); err != nil {
		return fmt.Errorf("sync dir: %w", err)
	}

	return nil
}

// Count returns the number of prefixes processed.
func (b *IndexBuilder) Count() uint64 {
	return b.posCount
}

// MaxDepth returns the maximum depth encountered.
func (b *IndexBuilder) MaxDepth() uint32 {
	return b.maxDepth
}

// PresentTiers returns the tier IDs that have data.
func (b *IndexBuilder) PresentTiers() []tiers.ID {
	result := make([]tiers.ID, 0, len(b.presentTiers))
	for tierID := range b.presentTiers {
		result = append(result, tierID)
	}
	return result
}
