package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/extsort/events"
	"github.com/rs/zerolog"
)

// drainSettleDelay gives in-flight events a brief window to land on the
// subscriber channel before the tracker closes its done signal.
const drainSettleDelay = 50 * time.Millisecond

// eventLogPerm restricts the JSONL event log to the running user;
// build events may include source bucket / inventory names.
const eventLogPerm = 0o600

// summaryFilePerm matches eventLogPerm — same rationale, plus the
// summary embeds peak RSS / per-stage timings.
const summaryFilePerm = 0o600

// buildTracker captures pipeline events during a build, writes optional
// JSONL event-log output, and emits a build.json sidecar next to the
// index files.
type buildTracker struct {
	outDir       string
	sourceURI    string
	eventLogPath string
	logger       *zerolog.Logger
	bus          *events.Bus
	startedAt    time.Time

	logFile *os.File
	logMu   sync.Mutex

	subCtx  chan struct{}
	subDone chan struct{}

	mu             sync.Mutex
	stageTimings   map[events.Stage]time.Duration
	stageRows      map[events.Stage]uint64
	stageBytes     map[events.Stage]uint64
	spillsWritten  int
	spillRows      uint64
	spillBytes     int64
	peakAllocBytes uint64
}

// newBuildTracker constructs a tracker. Pass eventLogPath="" to skip
// JSONL event logging.
func newBuildTracker(outDir, sourceURI, eventLogPath string, logger *zerolog.Logger) *buildTracker {
	return &buildTracker{
		outDir:       outDir,
		sourceURI:    sourceURI,
		eventLogPath: eventLogPath,
		logger:       logger,
		bus:          events.NewBus(),
		stageTimings: map[events.Stage]time.Duration{},
		stageRows:    map[events.Stage]uint64{},
		stageBytes:   map[events.Stage]uint64{},
	}
}

// wire returns the Observe block to inject into extsort.Config.
func (t *buildTracker) wire() extsort.ObserveConfig {
	return extsort.ObserveConfig{EventBus: t.bus}
}

// start opens the JSONL writer (if any) and launches a subscription
// goroutine that consumes every event.
func (t *buildTracker) start() {
	t.startedAt = time.Now()

	if t.eventLogPath != "" {
		f, err := os.OpenFile(t.eventLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, eventLogPerm)
		if err != nil {
			t.logger.Warn().Err(err).Str("path", t.eventLogPath).Msg("event log: open failed; continuing without log")
		} else {
			t.logFile = f
		}
	}

	sub := t.bus.Subscribe(1024)
	stop := make(chan struct{})
	t.subCtx = stop
	t.subDone = make(chan struct{})

	// Capture stop in the closures so they don't race with finish()
	// setting t.subCtx to nil after closing.
	go func() {
		defer close(t.subDone)
		for {
			select {
			case <-stop:
				// Cancel closes sub.C; the publisher uses a non-blocking
				// send so it cannot deadlock on any remaining buffered
				// values (they're simply dropped).
				sub.Cancel()

				return
			case ev, ok := <-sub.C:
				if !ok {
					return
				}
				t.handle(ev)
			}
		}
	}()

	// Background RSS poller updates peakAllocBytes during the build.
	go t.pollRSS(stop)
}

// pollRSS samples runtime.MemStats every second until stop closes. The
// stop channel is passed by value so finish() nilling t.subCtx cannot
// turn the select case into a wait-on-nil-channel.
func (t *buildTracker) pollRSS(stop <-chan struct{}) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			t.mu.Lock()
			if ms.Alloc > t.peakAllocBytes {
				t.peakAllocBytes = ms.Alloc
			}
			t.mu.Unlock()
		}
	}
}

func (t *buildTracker) handle(ev events.Event) {
	t.writeJSONL(ev)

	switch p := ev.Payload.(type) {
	case events.StageTiming:
		t.mu.Lock()
		t.stageTimings[p.Stage] += p.Duration
		t.stageRows[p.Stage] += p.Rows
		t.stageBytes[p.Stage] += p.Bytes
		t.mu.Unlock()
	case events.SpillCompleted:
		t.mu.Lock()
		t.spillsWritten++
		t.spillRows += p.Rows
		t.spillBytes += p.Bytes
		t.mu.Unlock()
	}
}

type jsonlEvent struct {
	Time    time.Time    `json:"time"`
	Stage   events.Stage `json:"stage"`
	Type    events.Type  `json:"type"`
	Payload any          `json:"payload,omitempty"`
}

func (t *buildTracker) writeJSONL(ev events.Event) {
	if t.logFile == nil {
		return
	}
	rec := jsonlEvent{Time: ev.Time, Stage: ev.Stage, Type: ev.Type, Payload: ev.Payload}
	t.logMu.Lock()
	defer t.logMu.Unlock()
	enc := json.NewEncoder(t.logFile)
	if err := enc.Encode(rec); err != nil {
		t.logger.Warn().Err(err).Msg("event log: write failed")
	}
}

// finish stops the subscription, writes the build.json sidecar, and
// closes the JSONL log if any. Safe to call once.
func (t *buildTracker) finish(result *extsort.Result, runErr error) {
	if t.subCtx == nil {
		return
	}

	// Allow in-flight events to land before we cancel.
	time.Sleep(drainSettleDelay)
	close(t.subCtx)
	t.subCtx = nil
	<-t.subDone

	summary := t.buildSummary(result, runErr)
	if err := t.writeSummary(summary); err != nil {
		t.logger.Warn().Err(err).Msg("build.json: write failed")
	}
}

// close releases the JSONL file. Idempotent.
func (t *buildTracker) close() {
	t.logMu.Lock()
	defer t.logMu.Unlock()
	if t.logFile != nil {
		_ = t.logFile.Close()
		t.logFile = nil
	}
}

type buildStageRecord struct {
	Stage    string        `json:"stage"`
	Duration time.Duration `json:"duration_ns"`
	Rows     uint64        `json:"rows"`
	Bytes    uint64        `json:"bytes"`
}

type buildSummary struct {
	Source           string             `json:"source"`
	StartedAt        time.Time          `json:"started_at"`
	FinishedAt       time.Time          `json:"finished_at"`
	Duration         time.Duration      `json:"duration_ns"`
	Outcome          string             `json:"outcome"`
	Error            string             `json:"error,omitempty"`
	NodeCount        uint64             `json:"node_count,omitempty"`
	MaxDepth         uint32             `json:"max_depth,omitempty"`
	ChunksProcessed  int                `json:"chunks_processed"`
	ObjectsProcessed int64              `json:"objects_processed"`
	RunFilesCreated  int                `json:"run_files_created"`
	SpillsWritten    int                `json:"spills_written"`
	SpillRows        uint64             `json:"spill_rows"`
	SpillBytes       int64              `json:"spill_bytes"`
	PeakAllocBytes   uint64             `json:"peak_alloc_bytes"`
	GoVersion        string             `json:"go_version"`
	NumCPU           int                `json:"num_cpu"`
	Stages           []buildStageRecord `json:"stages"`
}

func (t *buildTracker) buildSummary(result *extsort.Result, runErr error) buildSummary {
	t.mu.Lock()
	stages := make([]buildStageRecord, 0, len(t.stageTimings))
	for stage, d := range t.stageTimings {
		stages = append(stages, buildStageRecord{
			Stage:    string(stage),
			Duration: d,
			Rows:     t.stageRows[stage],
			Bytes:    t.stageBytes[stage],
		})
	}
	spills := t.spillsWritten
	spillRows := t.spillRows
	spillBytes := t.spillBytes
	peakAlloc := t.peakAllocBytes
	t.mu.Unlock()

	now := time.Now()
	outcome := "success"
	errStr := ""
	if runErr != nil {
		outcome = "failure"
		errStr = runErr.Error()
	}

	s := buildSummary{
		Source:         t.sourceURI,
		StartedAt:      t.startedAt,
		FinishedAt:     now,
		Duration:       now.Sub(t.startedAt),
		Outcome:        outcome,
		Error:          errStr,
		SpillsWritten:  spills,
		SpillRows:      spillRows,
		SpillBytes:     spillBytes,
		PeakAllocBytes: peakAlloc,
		GoVersion:      runtime.Version(),
		NumCPU:         runtime.NumCPU(),
		Stages:         stages,
	}
	if result != nil {
		s.NodeCount = result.PrefixCount
		s.MaxDepth = result.MaxDepth
		s.ChunksProcessed = result.ChunksProcessed
		s.ObjectsProcessed = result.ObjectsProcessed
		s.RunFilesCreated = result.RunFilesCreated
	}

	return s
}

func (t *buildTracker) writeSummary(s buildSummary) error {
	path := filepath.Join(t.outDir, "build.json")
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal summary: %w", err)
	}
	if err := os.WriteFile(path, data, summaryFilePerm); err != nil {
		return fmt.Errorf("write build summary: %w", err)
	}

	return nil
}
