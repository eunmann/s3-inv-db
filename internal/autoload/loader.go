package autoload

import (
	"context"
	"fmt"

	"github.com/eunmann/s3-inv-db/internal/inventory"
)

// DiscoveryLoader adapts DiscoveryService.AutoLoadWith to the Loader
// interface AutoLoader consumes. Strips the progress-callback knob
// (auto-loads don't have a UI watcher) and pins=false (already implied
// by AutoLoadWith).
type DiscoveryLoader struct {
	d *inventory.DiscoveryService
}

// NewDiscoveryLoader wraps a DiscoveryService.
func NewDiscoveryLoader(d *inventory.DiscoveryService) *DiscoveryLoader {
	return &DiscoveryLoader{d: d}
}

// AutoLoad runs DiscoveryService.AutoLoadWith with a nil progress
// callback.
func (l *DiscoveryLoader) AutoLoad(ctx context.Context, disc inventory.Inventory) error {
	if err := l.d.AutoLoadWith(ctx, disc, nil); err != nil {
		return fmt.Errorf("auto-load %s: %w", disc.CompositeID(), err)
	}
	return nil
}
