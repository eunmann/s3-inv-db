// Package autoload polls S3 for new inventory runs and queues loads
// for configurations whose auto_load flag is set.
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

// Config holds the AutoLoader's runtime knobs. Zero values pick
// sensible defaults (15m poll, 1 concurrent load, 1m–1h backoff).
type Config struct {
	PollInterval     time.Duration
	MaxConcurrency   int
	MinBackoff       time.Duration
	MaxBackoff       time.Duration
	DefaultRetention uint32
}

type Discovery interface {
	Enabled() bool
	List(ctx context.Context) ([]inventory.MergedInventory, error)
}

type Loader interface {
	AutoLoad(ctx context.Context, disc inventory.Inventory) error
}

// AutoLoader polls Discovery on a ticker and feeds new runs into Loader.
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

// New constructs an AutoLoader; logger may be nil.
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

// Start launches the poll loop.
func (a *AutoLoader) Start(ctx context.Context) {
	a.wg.Add(1)
	go a.run(ctx)
}

// Stop signals the poll loop to exit and waits.
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

func (a *AutoLoader) pickTargets(byConfig map[string][]inventory.MergedInventory, enabled map[string]inventory.Config) []inventory.Inventory {
	var queue []inventory.Inventory
	for cfgID := range enabled {
		runs := byConfig[cfgID]
		if len(runs) == 0 {
			continue
		}
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
	info, _ := a.manager.Get(id)
	delay := backoffDelay(a.cfg.MinBackoff, a.cfg.MaxBackoff, info.AutoLoadFailureCount)
	retryAt := a.now().Add(delay)
	_ = a.manager.RecordAutoLoadFailure(id, err.Error(), retryAt)
	a.logger.Error().Str("id", string(id)).Time("retry_at", retryAt).Err(err).Msg("autoload: failed; backing off")
}

func (a *AutoLoader) recordPollFailure(enabled map[string]inventory.Config, msg string) {
	for _, c := range enabled {
		c.PollFailureCount++
		c.LastPollError = msg
		c.PollBackoffUntil = a.now().Add(backoffDelay(a.cfg.MinBackoff, a.cfg.MaxBackoff, c.PollFailureCount))
		_ = a.configStore.Upsert(c)
	}
}

// backoffDelay returns minBackoff * 2^count, clamped to maxBackoff and
// capped against shift overflow when count grows beyond int64 width.
func backoffDelay(minBackoff, maxBackoff time.Duration, count uint32) time.Duration {
	if count >= 32 {
		return maxBackoff
	}
	delay := minBackoff << count
	if delay <= 0 || delay > maxBackoff {
		return maxBackoff
	}

	return delay
}
