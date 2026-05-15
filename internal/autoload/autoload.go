// Package autoload runs a background poller that discovers new
// inventory runs on S3 and queues loads for any configuration whose
// auto_load flag is set in inventory_configs. Per-config retention
// shrinks back to the configured limit; the disk-budget Gate enforces
// the global cap and refuses politely (surfaced in the UI) when there
// isn't room.
package autoload

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/loadgate"
	"github.com/rs/zerolog"
)

// Config holds the AutoLoader's runtime knobs.
type Config struct {
	// PollInterval is the wall-clock period between discovery passes.
	// Default 15m if zero.
	PollInterval time.Duration

	// MaxConcurrency caps how many in-flight auto-loads can run at
	// once. Default 1 if zero.
	MaxConcurrency int

	// MinBackoff and MaxBackoff bound the per-run exponential backoff
	// applied after a failed auto-load. Defaults: 1m / 1h.
	MinBackoff, MaxBackoff time.Duration

	// DefaultRetention applies when an inventory_configs row sets
	// retention_count = 0.
	DefaultRetention uint32
}

// Discovery is the surface AutoLoader uses on the discovery service.
// Defined as an interface so tests can plug in a fake without
// constructing a real DiscoveryService.
type Discovery interface {
	Enabled() bool
	List(ctx context.Context) ([]inventory.MergedInventory, error)
}

// Loader runs one gated auto-load for a discovered run.
type Loader interface {
	AutoLoad(ctx context.Context, disc inventory.Inventory) error
}

// AutoLoader is the background service. Start spawns the ticker
// goroutine and returns immediately. Stop drains the queue and
// returns when in-flight loads are done.
type AutoLoader struct {
	cfg         Config
	discovery   Discovery
	loader      Loader
	configStore *inventory.ConfigStore
	manager     *inventory.Manager
	logger      *zerolog.Logger

	now func() time.Time

	mu      sync.Mutex
	stopped bool
	wg      sync.WaitGroup
	stop    chan struct{}
}

// New constructs an AutoLoader. Required arguments are non-nil; the
// logger may be nil (a no-op logger is used).
func New(cfg Config, discovery Discovery, loader Loader, configStore *inventory.ConfigStore, manager *inventory.Manager, logger *zerolog.Logger) *AutoLoader {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 15 * time.Minute
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 1
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = time.Minute
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = time.Hour
	}
	if cfg.DefaultRetention == 0 {
		cfg.DefaultRetention = inventory.DefaultRetentionCount
	}
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}
	return &AutoLoader{
		cfg:         cfg,
		discovery:   discovery,
		loader:      loader,
		configStore: configStore,
		manager:     manager,
		logger:      logger,
		now:         time.Now,
		stop:        make(chan struct{}),
	}
}

// Start launches the poll loop. Safe to call once.
func (a *AutoLoader) Start(ctx context.Context) {
	a.wg.Add(1)
	go a.run(ctx)
}

// Stop signals the poll loop to exit and waits for it.
func (a *AutoLoader) Stop() {
	a.mu.Lock()
	if a.stopped {
		a.mu.Unlock()
		return
	}
	a.stopped = true
	close(a.stop)
	a.mu.Unlock()
	a.wg.Wait()
}

func (a *AutoLoader) run(ctx context.Context) {
	defer a.wg.Done()
	// Run an immediate pass so a freshly-started server doesn't wait
	// the full interval before the first auto-load attempt.
	a.tick(ctx)
	t := time.NewTicker(a.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stop:
			return
		case <-t.C:
			a.tick(ctx)
		}
	}
}

// tick runs one discovery pass.
func (a *AutoLoader) tick(ctx context.Context) {
	if a.discovery == nil || !a.discovery.Enabled() {
		return
	}
	configs, err := a.configStore.List()
	if err != nil {
		a.logger.Error().Err(err).Msg("autoload: list configs")
		return
	}
	enabled := map[string]inventory.Config{}
	for _, c := range configs {
		if c.AutoLoad && (c.PollBackoffUntil.IsZero() || a.now().After(c.PollBackoffUntil)) {
			enabled[c.ConfigID()] = c
		}
	}
	if len(enabled) == 0 {
		return
	}
	merged, err := a.discovery.List(ctx)
	if err != nil {
		a.logger.Error().Err(err).Msg("autoload: discover")
		a.recordPollFailure(enabled, err.Error())
		return
	}
	// Group by config; pick the newest run per group whose composite
	// ID isn't already loaded and isn't user-unloaded and isn't in
	// per-run backoff.
	byConfig := map[string][]inventory.MergedInventory{}
	for i := range merged {
		key := merged[i].ConfigID()
		byConfig[key] = append(byConfig[key], merged[i])
	}
	queue := a.pickTargets(byConfig, enabled)
	for k := range enabled {
		c := enabled[k]
		c.LastPolledAt = a.now()
		c.PollFailureCount = 0
		c.LastPollError = ""
		c.PollBackoffUntil = time.Time{}
		_ = a.configStore.Upsert(c)
	}
	a.runQueue(ctx, queue)
}

// pickTargets returns one target inventory per config — the newest
// completed run that hasn't been loaded, isn't sticky-unloaded, and
// isn't currently within its auto-load backoff window.
func (a *AutoLoader) pickTargets(byConfig map[string][]inventory.MergedInventory, enabled map[string]inventory.Config) []inventory.Inventory {
	var queue []inventory.Inventory
	for cfgID := range enabled {
		runs := byConfig[cfgID]
		if len(runs) == 0 {
			continue
		}
		// Discovery returns newest-first within a config, but enforce
		// order so we don't depend on that for correctness.
		sort.SliceStable(runs, func(i, j int) bool { return runs[i].Run > runs[j].Run })
		var target inventory.Inventory
		for i := range runs {
			r := &runs[i]
			if r.Run == "" || r.ManifestKey == "" {
				continue
			}
			info, ok := a.manager.Get(r.CompositeID())
			if ok {
				if info.State == inventory.StateLoaded || info.State == inventory.StateLoading {
					continue
				}
				if !info.UserUnloadedAt.IsZero() {
					continue
				}
				if !info.AutoLoadBackoffUntil.IsZero() && a.now().Before(info.AutoLoadBackoffUntil) {
					continue
				}
			}
			target = r.Inventory
			break
		}
		if target.Run != "" {
			queue = append(queue, target)
		}
	}
	return queue
}

// runQueue loads each target through the gated loader, respecting
// MaxConcurrency. Per-target failures get exponential backoff.
func (a *AutoLoader) runQueue(ctx context.Context, queue []inventory.Inventory) {
	if len(queue) == 0 {
		return
	}
	sem := make(chan struct{}, a.cfg.MaxConcurrency)
	var wg sync.WaitGroup
	for _, target := range queue {
		select {
		case <-ctx.Done():
			return
		case <-a.stop:
			return
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(target inventory.Inventory) {
			defer wg.Done()
			defer func() { <-sem }()
			a.loadOne(ctx, target)
		}(target)
	}
	wg.Wait()
}

func (a *AutoLoader) loadOne(ctx context.Context, target inventory.Inventory) {
	id := target.CompositeID()
	err := a.loader.AutoLoad(ctx, target)
	if err == nil {
		a.logger.Info().Str("id", string(id)).Msg("autoload: loaded")
		return
	}
	var refused *loadgate.BudgetRefusedError
	if errors.As(err, &refused) {
		// Budget refusal: surface via Manager so the UI can show the
		// reason next to the row. Don't apply backoff — we want to
		// retry next tick if budget frees up.
		_ = a.manager.RecordAutoLoadFailure(id, refused.Error(), time.Time{})
		a.logger.Warn().Str("id", string(id)).Err(err).Msg("autoload: budget refused")
		return
	}
	// Other failures: exponential backoff per-run.
	info, _ := a.manager.Get(id)
	count := info.AutoLoadFailureCount
	delay := a.cfg.MinBackoff << count
	if delay > a.cfg.MaxBackoff || delay <= 0 {
		delay = a.cfg.MaxBackoff
	}
	retryAt := a.now().Add(delay)
	_ = a.manager.RecordAutoLoadFailure(id, err.Error(), retryAt)
	a.logger.Error().Str("id", string(id)).Time("retry_at", retryAt).Err(err).Msg("autoload: failed; backing off")
}

func (a *AutoLoader) recordPollFailure(enabled map[string]inventory.Config, msg string) {
	for _, c := range enabled {
		c.PollFailureCount++
		c.LastPollError = msg
		delay := a.cfg.MinBackoff << c.PollFailureCount
		if delay > a.cfg.MaxBackoff || delay <= 0 {
			delay = a.cfg.MaxBackoff
		}
		c.PollBackoffUntil = a.now().Add(delay)
		_ = a.configStore.Upsert(c)
	}
}
