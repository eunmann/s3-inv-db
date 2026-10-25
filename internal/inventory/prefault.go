package inventory

import (
	"context"
	"fmt"

	"github.com/eunmann/s3-inv-db/pkg/format"
	"github.com/eunmann/s3-inv-db/pkg/indexread"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
)

// prefaultMemoryShare is how much of the memory the server can use
// (sysmem.Available: host RAM, or the container's limit) the indexes it
// prefaulted may take together: at most 1/8, so reading one in doesn't
// push the other loaded indexes out of the page cache, and a restart
// reads at most that much before it serves.
const prefaultMemoryShare = 8

// prefaultLimit is the catalog's prefault budget in bytes.
func prefaultLimit() uint64 {
	return sysmem.Available().TotalBytes / prefaultMemoryShare
}

// planPrefaultLocked decides whether inv prefaults the index in dir
// when it opens it: the index's manifest size must fit in what is left
// of c.prefaultLimit after the indexes already prefaulted by loaded or
// loading inventories. It records the size on inv and returns the
// manifest to prefault, or nil (also without a readable manifest).
// Caller holds c.mu.
func (c *Catalog) planPrefaultLocked(inv *managedInventory, dir string) *format.Manifest {
	inv.prefaultBytes = 0
	m, err := format.ReadManifest(dir)
	if err != nil {
		return nil
	}
	var used uint64
	for _, other := range c.inventories {
		if other != inv && (other.index != nil || other.info.State == StateLoading) {
			used += other.prefaultBytes
		}
	}
	size := m.TotalBytes()
	if used > c.prefaultLimit || size > c.prefaultLimit-used {
		return nil
	}
	inv.prefaultBytes = size

	return m
}

// openServed opens an index the server will serve, after reading the
// files m lists into the page cache when m is non-nil, so the first
// queries after a load or a restart don't wait on storage (open plus
// the first 1000 lookups -53% on the benchmark fixture, #55).
// Prefaulting only warms the cache: when it fails, the index opens as
// usual and its pages fault in as queries touch them.
func (c *Catalog) openServed(dir string, m *format.Manifest) (*indexread.Index, error) {
	if m != nil {
		if err := c.prefault(dir, m); err != nil {
			logging.L().Warn().Err(err).Str("dir", dir).Msg("prefault index; opening without it")
		}
	}
	idx, err := indexread.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("open index: %w", err)
	}

	return idx, nil
}

// PrefaultRestored reads the indexes Hydrate restored into the page
// cache, one at a time, skipping any no longer served (unloaded or
// replaced since). The server runs it once it is listening, so a
// restart answers queries right away; until an index is warmed, its
// pages fault in as queries touch them, as they would without
// prefaulting.
func (c *Catalog) PrefaultRestored(ctx context.Context) {
	c.mu.Lock()
	pending := c.restored
	c.restored = nil
	c.mu.Unlock()
	for _, r := range pending {
		if ctx.Err() != nil {
			return
		}
		c.mu.RLock()
		cur, ok := c.inventories[r.inv.info.ID]
		served := ok && cur == r.inv && cur.index == r.index
		c.mu.RUnlock()
		if !served {
			continue
		}
		if err := c.prefault(r.dir, r.manifest); err != nil {
			logging.L().Warn().Err(err).Str("dir", r.dir).Msg("prefault restored index")
		}
	}
}
