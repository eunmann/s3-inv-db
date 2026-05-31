// Package autoload polls S3 for new inventory runs and queues loads
// for configurations whose auto_load flag is set.
package autoload

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/internal/inventory"
	"github.com/eunmann/s3-inv-db/internal/jobs"
	"github.com/eunmann/s3-inv-db/internal/loadcontrol"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/rs/zerolog"
)

// DefaultPollInterval is the fallback polling interval for an
// AutoLoader created with a zero PollInterval. Exported so the
// server binary's CLI flag default stays in sync.
const DefaultPollInterval = 15 * time.Minute

// Backoff bounds for failed polls and failed loads. Fixed: the only
// callers that varied them were unit tests, and the curve (1m base,
// 1h ceiling, doubling) suits the polling cadence regardless of deploy.
const (
	minBackoff = time.Minute
	maxBackoff = time.Hour
)

// Config holds the AutoLoader's runtime knobs. Zero values pick
// sensible defaults (15m poll, 1m–1h backoff). Concurrency is owned by
// the jobs scheduler, not the AutoLoader.
type Config struct {
	PollInterval     time.Duration
	DefaultRetention uint32
}

type Discovery interface {
	Enabled() bool
	List(ctx context.Context) ([]inventory.MergedInventory, error)
}

// LoaderFunc loads a single discovered inventory, reporting progress.
// Callers wire Discovery.AutoLoadWith here. The eventBus, if non-nil,
// is plumbed to the build pipeline so jobs.Recorder can collect a
// per-stage timeline for the drawer.
type LoaderFunc func(ctx context.Context, disc inventory.Inventory, onProgress func(stage string, done, total int64), eventBus *events.Bus) error

// Submitter routes a load through the jobs scheduler so auto-loads get
// the same tracking, dedup, and concurrency limiting as manual builds.
type Submitter interface {
	Submit(ctx context.Context, invID inventory.ID, kind jobs.Kind, work jobs.Work) (jobs.Job, error)
}

// AutoLoader polls Discovery on a ticker and submits new runs as jobs.
type AutoLoader struct {
	discovery   Discovery
	loader      LoaderFunc
	submitter   Submitter
	configStore *inventory.ConfigStore
	manager     *inventory.Catalog
	logger      *zerolog.Logger
	now         func() time.Time
	stop        chan struct{}
	cfg         Config
	wg          sync.WaitGroup
	mu          sync.Mutex
	stopped     bool
}

// Deps groups the required collaborators for an AutoLoader. All
// fields must be non-nil; nil values produce nil-deref panics on first
// use rather than at construction.
type Deps struct {
	Discovery   Discovery
	Loader      LoaderFunc
	Submitter   Submitter
	ConfigStore *inventory.ConfigStore
	Manager     *inventory.Catalog
}

// New constructs an AutoLoader; logger may be nil.
func New(cfg Config, deps Deps, logger *zerolog.Logger) *AutoLoader {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
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
		discovery:   deps.Discovery,
		loader:      deps.Loader,
		submitter:   deps.Submitter,
		configStore: deps.ConfigStore,
		manager:     deps.Manager,
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
	if !a.discovery.Enabled() {
		return
	}
	configs, err := a.configStore.List(ctx)
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
		a.recordPollFailure(ctx, enabled, err.Error())

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
		if err := a.configStore.Upsert(ctx, c); err != nil {
			a.logger.Warn().Err(err).Str("config_id", c.ConfigID()).Msg("autoload: persist poll-success state")
		}
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
		slices.SortStableFunc(runs, func(a, b inventory.MergedInventory) int { return strings.Compare(b.Run, a.Run) })
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

// runQueue submits each target as a build job. Submit returns
// immediately; the scheduler owns concurrency limiting and dedup, so a
// run already building (manually or from a prior tick) is rejected with
// ErrDuplicateInventory and skipped.
func (a *AutoLoader) runQueue(ctx context.Context, queue []inventory.Inventory) {
	for i := range queue {
		select {
		case <-ctx.Done():
			return
		case <-a.stop:
			return
		default:
		}
		a.loadOne(ctx, queue[i])
	}
}

func (a *AutoLoader) loadOne(ctx context.Context, target inventory.Inventory) {
	id := target.CompositeID()
	work := func(jobCtx context.Context, report func(jobs.Update)) error {
		// Recorder translates extsort pipeline events into Update.Stages so
		// the drawer's per-stage timeline is populated for auto-loaded runs
		// the same way it is for manual loads.
		recorder := jobs.NewRecorder(report)
		defer recorder.Close()
		err := a.loader(jobCtx, target, recorder.OnProgress, recorder.Bus())
		if err != nil {
			// A cancelled load (server shutdown) is not a load failure and
			// must not consume the backoff budget.
			if jobCtx.Err() == nil {
				a.recordLoadFailure(jobCtx, id, err)
			}

			return err
		}
		a.logger.Info().Str("id", string(id)).Msg("autoload: loaded")

		return nil
	}

	_, err := a.submitter.Submit(ctx, id, jobs.KindBuild, work)
	switch {
	case err == nil, errors.Is(err, jobs.ErrDuplicateInventory):
		// Submitted, or already building — nothing more to do.
	case errors.Is(err, jobs.ErrShutdown):
		a.logger.Debug().Str("id", string(id)).Msg("autoload: scheduler shut down, skipping submit")
	default:
		a.logger.Error().Str("id", string(id)).Err(err).Msg("autoload: submit failed")
	}
}

// recordLoadFailure applies the budget-refusal or exponential-backoff
// bookkeeping for a failed auto-load. Runs inside the job's work
// closure, where the load outcome is observed.
func (a *AutoLoader) recordLoadFailure(ctx context.Context, id inventory.ID, err error) {
	var refused *loadcontrol.BudgetRefusedError
	if errors.As(err, &refused) {
		// Budget refusal: surface via Manager so the UI can show the
		// reason next to the row. Don't apply backoff — we want to
		// retry next tick if budget frees up.
		_ = a.manager.RecordAutoLoadFailure(ctx, id, refused.Error(), a.now(), time.Time{})
		a.logger.Warn().Str("id", string(id)).Err(err).Msg("autoload: budget refused")

		return
	}
	info, _ := a.manager.Get(id)
	now := a.now()
	delay := backoffDelay(minBackoff, maxBackoff, info.AutoLoadFailureCount)
	retryAt := now.Add(delay)
	_ = a.manager.RecordAutoLoadFailure(ctx, id, err.Error(), now, retryAt)
	a.logger.Error().Str("id", string(id)).Time("retry_at", retryAt).Err(err).Msg("autoload: failed; backing off")
}

func (a *AutoLoader) recordPollFailure(ctx context.Context, enabled map[string]inventory.Config, msg string) {
	for _, c := range enabled {
		c.PollFailureCount++
		c.LastPollError = msg
		c.PollBackoffUntil = a.now().Add(backoffDelay(minBackoff, maxBackoff, c.PollFailureCount))
		if err := a.configStore.Upsert(ctx, c); err != nil {
			a.logger.Warn().Err(err).Str("config_id", c.ConfigID()).Msg("autoload: persist poll-failure state")
		}
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
