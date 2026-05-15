package logging_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/rs/zerolog"
)

func TestProgressTracker_BasicOperations(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	pt := logging.NewProgressTracker("test_phase", 10, log)

	// Record some completions.
	pt.RecordCompletion(100 * time.Millisecond)
	pt.RecordCompletion(150 * time.Millisecond)
	pt.RecordSkip()

	snap := pt.Progress()
	if snap.Completed != 2 {
		t.Errorf("expected completed=2, got %d", snap.Completed)
	}
	if snap.Skipped != 1 {
		t.Errorf("expected skipped=1, got %d", snap.Skipped)
	}
	if snap.Total != 10 {
		t.Errorf("expected total=10, got %d", snap.Total)
	}

	pct := pt.ProgressPct()
	if pct != 30.0 { // (2+1)/10 * 100
		t.Errorf("expected progress 30%%, got %.1f%%", pct)
	}

	remaining := pt.Remaining()
	if remaining != 7 { // 10 - 2 - 1
		t.Errorf("expected remaining=7, got %d", remaining)
	}
}

func TestProgressTracker_ETA(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	pt := logging.NewProgressTracker("test_phase", 10, log)

	// Record completions with known duration.
	pt.RecordCompletion(100 * time.Millisecond)
	pt.RecordCompletion(100 * time.Millisecond)

	eta := pt.ETA()
	// With 2 completed at 100ms each, 8 remaining should be ~800ms.
	if eta < 700*time.Millisecond || eta > 900*time.Millisecond {
		t.Errorf("expected ETA ~800ms, got %v", eta)
	}
}

func TestProgressTracker_ZeroTotal(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	pt := logging.NewProgressTracker("test_phase", 0, log)

	pct := pt.ProgressPct()
	if pct != 100.0 {
		t.Errorf("expected 100%% for zero total, got %.1f%%", pct)
	}

	eta := pt.ETA()
	if eta != 0 {
		t.Errorf("expected 0 ETA for zero total, got %v", eta)
	}
}

func TestCompletionEvent_BasicFields(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	ce := logging.NewCompletionEvent(log, "test_event", "test_phase", 500*time.Millisecond, false)
	ce.Str("key", "value").
		Int("count", 42).
		Int64("big_count", 1000000).
		Log("test message")

	output := buf.String()

	if !strings.Contains(output, `"event":"test_event"`) {
		t.Errorf("expected event field, got: %s", output)
	}
	if !strings.Contains(output, `"phase":"test_phase"`) {
		t.Errorf("expected phase field, got: %s", output)
	}
	if !strings.Contains(output, `"duration_ms":500`) {
		t.Errorf("expected duration_ms field, got: %s", output)
	}
	if !strings.Contains(output, `"key":"value"`) {
		t.Errorf("expected key field, got: %s", output)
	}
	if !strings.Contains(output, `"count":42`) {
		t.Errorf("expected count field, got: %s", output)
	}
}

func TestCompletionEvent_BytesAndCounts(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	ce := logging.NewCompletionEvent(log, "test_event", "test_phase", 1*time.Second, true)
	ce.Bytes("size", 1073741824). // 1 GiB
					Count("items", 1500000).
					Log("test message")

	output := buf.String()

	if !strings.Contains(output, `"size":1073741824`) {
		t.Errorf("expected raw size field, got: %s", output)
	}
	if !strings.Contains(output, `"items":1500000`) {
		t.Errorf("expected raw items field, got: %s", output)
	}

	// Pretty mode is on so human-readable companions must be present.
	if !strings.Contains(output, `"size_h":"1.00 GiB"`) {
		t.Errorf("expected human size field, got: %s", output)
	}
	if !strings.Contains(output, `"items_h":"1.5M"`) {
		t.Errorf("expected human items field, got: %s", output)
	}
}

func TestCompletionEvent_Progress(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	ce := logging.NewCompletionEvent(log, "test_event", "test_phase", 1*time.Second, true)
	ce.Progress(50, 100, 30*time.Second).
		Log("test message")

	output := buf.String()

	if !strings.Contains(output, `"done":50`) {
		t.Errorf("expected done field, got: %s", output)
	}
	if !strings.Contains(output, `"total":100`) {
		t.Errorf("expected total field, got: %s", output)
	}
	if !strings.Contains(output, `"progress_pct":50`) {
		t.Errorf("expected progress_pct field, got: %s", output)
	}
	if !strings.Contains(output, `"eta_ms":30000`) {
		t.Errorf("expected eta_ms field, got: %s", output)
	}
	if !strings.Contains(output, `"eta_h":`) {
		t.Errorf("expected eta_h field in pretty mode, got: %s", output)
	}
}

func TestCompletionEvent_ProgressFromTracker(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	pt := logging.NewProgressTracker("test_phase", 100, log)
	pt.RecordCompletion(100 * time.Millisecond)
	pt.RecordCompletion(100 * time.Millisecond)
	pt.RecordSkip()

	ce := logging.NewCompletionEvent(log, "test_event", "test_phase", 1*time.Second, false)
	ce.ProgressFromTracker(pt).
		Log("test message")

	output := buf.String()

	if !strings.Contains(output, `"completed":2`) {
		t.Errorf("expected completed field, got: %s", output)
	}
	if !strings.Contains(output, `"skipped":1`) {
		t.Errorf("expected skipped field, got: %s", output)
	}
	if !strings.Contains(output, `"total":100`) {
		t.Errorf("expected total field, got: %s", output)
	}
}

func TestCompletionEvent_Throughput(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	ce := logging.NewCompletionEvent(log, "test_event", "test_phase", 1*time.Second, true)
	ce.Throughput(104857600). // 100 MiB in 1 second = 100 MiB/s
					Log("test message")

	output := buf.String()

	if !strings.Contains(output, `"throughput_bps":`) {
		t.Errorf("expected throughput_bps field, got: %s", output)
	}
	if !strings.Contains(output, `"throughput_h":"100.00 MiB/s"`) {
		t.Errorf("expected throughput_h field, got: %s", output)
	}
}

func TestHelperFunctions(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	// Test PhaseComplete.
	logging.PhaseComplete(log, "test_phase", 1*time.Second, false).
		Str("key", "value").
		Log("phase done")

	output := buf.String()
	if !strings.Contains(output, `"event":"phase_completed"`) {
		t.Errorf("expected phase_completed event, got: %s", output)
	}

	// Test ChunkComplete.
	buf.Reset()
	logging.ChunkComplete(log, "test_phase", 500*time.Millisecond, false).
		Str("chunk_id", "chunk1").
		Log("chunk done")

	output = buf.String()
	if !strings.Contains(output, `"event":"chunk_completed"`) {
		t.Errorf("expected chunk_completed event, got: %s", output)
	}

	// Test BatchComplete.
	buf.Reset()
	logging.BatchComplete(log, "test_phase", 200*time.Millisecond, false).
		Int("batch_size", 1000).
		Log("batch done")

	output = buf.String()
	if !strings.Contains(output, `"event":"batch_completed"`) {
		t.Errorf("expected batch_completed event, got: %s", output)
	}

	// Test FileCreated.
	buf.Reset()
	logging.FileCreated(log, "test_phase", 100*time.Millisecond, false).
		Str("file", "test.bin").
		Log("file done")

	output = buf.String()
	if !strings.Contains(output, `"event":"file_created"`) {
		t.Errorf("expected file_created event, got: %s", output)
	}
}

func TestCompletionEvent_LogDebug(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf).Level(zerolog.DebugLevel)

	// Temporarily lower global level to allow debug output.
	oldLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(oldLevel)

	ce := logging.NewCompletionEvent(log, "test_event", "test_phase", 1*time.Second, false)
	ce.LogDebug("debug message")

	output := buf.String()
	if !strings.Contains(output, `"level":"debug"`) {
		t.Errorf("expected debug level, got: %s", output)
	}
}

func TestChunkStarted(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	logging.ChunkStarted(log, "aggregate", "chunk-001", 5, 100)

	output := buf.String()

	if !strings.Contains(output, `"event":"chunk_started"`) {
		t.Errorf("expected event field, got: %s", output)
	}
	if !strings.Contains(output, `"phase":"aggregate"`) {
		t.Errorf("expected phase field, got: %s", output)
	}
	if !strings.Contains(output, `"chunk_id":"chunk-001"`) {
		t.Errorf("expected chunk_id field, got: %s", output)
	}
	if !strings.Contains(output, `"chunks_complete":5`) {
		t.Errorf("expected chunks_complete field, got: %s", output)
	}
	if !strings.Contains(output, `"chunks_total":100`) {
		t.Errorf("expected chunks_total field, got: %s", output)
	}
	if strings.Contains(output, `"progress_pct"`) {
		t.Errorf("chunk_started should not have progress_pct, got: %s", output)
	}
}

func TestProgressTracker_MultipleChunks(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	pt := logging.NewProgressTracker("test_phase", 10, log)

	pt.RecordSkip()
	pt.RecordSkip()

	pt.RecordCompletion(100 * time.Millisecond)
	pt.RecordCompletion(120 * time.Millisecond)
	pt.RecordCompletion(80 * time.Millisecond)

	snap := pt.Progress()
	if snap.Completed != 3 {
		t.Errorf("expected completed=3, got %d", snap.Completed)
	}
	if snap.Skipped != 2 {
		t.Errorf("expected skipped=2, got %d", snap.Skipped)
	}
	if snap.Total != 10 {
		t.Errorf("expected total=10, got %d", snap.Total)
	}

	pct := pt.ProgressPct()
	if pct != 50.0 {
		t.Errorf("expected progress 50%%, got %.1f%%", pct)
	}

	remaining := pt.Remaining()
	if remaining != 5 {
		t.Errorf("expected remaining=5, got %d", remaining)
	}

	if pt.Completed() != 3 {
		t.Errorf("expected Completed()=3, got %d", pt.Completed())
	}

	if pt.Total() != 10 {
		t.Errorf("expected Total()=10, got %d", pt.Total())
	}
}

func TestProgressTracker_ChunkCompletedAfterCommit(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	pt := logging.NewProgressTracker("aggregate", 3, log)

	time.Sleep(10 * time.Millisecond)
	pt.RecordCompletion(10 * time.Millisecond)

	pct := pt.ProgressPct()
	if pct < 33.0 || pct > 34.0 {
		t.Errorf("expected ~33%% after 1 chunk, got %.2f%%", pct)
	}

	time.Sleep(10 * time.Millisecond)
	pt.RecordCompletion(10 * time.Millisecond)

	pct = pt.ProgressPct()
	if pct < 66.0 || pct > 67.0 {
		t.Errorf("expected ~67%% after 2 chunks, got %.2f%%", pct)
	}

	time.Sleep(10 * time.Millisecond)
	pt.RecordCompletion(10 * time.Millisecond)

	pct = pt.ProgressPct()
	if pct != 100.0 {
		t.Errorf("expected 100%% after all chunks, got %.2f%%", pct)
	}
}

func TestCompletionEvent_OnlyAfterCommit(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	pt := logging.NewProgressTracker("aggregate", 10, log)
	pt.RecordCompletion(500 * time.Millisecond)

	ce := logging.ChunkComplete(log, "aggregate", 500*time.Millisecond, false)
	ce.Str("chunk_id", "test-chunk").
		Count("objects", 50000).
		Bytes("bytes", 1073741824).
		ProgressFromTracker(pt).
		Throughput(1073741824).
		Log("chunk committed to SQLite")

	output := buf.String()

	if !strings.Contains(output, `"event":"chunk_completed"`) {
		t.Errorf("expected event=chunk_completed, got: %s", output)
	}
	if !strings.Contains(output, `"chunk_id":"test-chunk"`) {
		t.Errorf("expected chunk_id, got: %s", output)
	}
	if !strings.Contains(output, `"objects":50000`) {
		t.Errorf("expected objects, got: %s", output)
	}
	if !strings.Contains(output, `"bytes":1073741824`) {
		t.Errorf("expected bytes, got: %s", output)
	}
	if !strings.Contains(output, `"completed":1`) {
		t.Errorf("expected completed=1, got: %s", output)
	}
	if !strings.Contains(output, `"total":10`) {
		t.Errorf("expected total=10, got: %s", output)
	}
	if !strings.Contains(output, `"progress_pct":10`) {
		t.Errorf("expected progress_pct=10, got: %s", output)
	}
	if !strings.Contains(output, `"duration_ms":500`) {
		t.Errorf("expected duration_ms, got: %s", output)
	}
}
