package logging

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestProgressTracker_BasicOperations(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	pt := NewProgressTracker("test_phase", 10, log)

	// Record some completions
	pt.RecordCompletion(100 * time.Millisecond)
	pt.RecordCompletion(150 * time.Millisecond)
	pt.RecordSkip()

	completed, skipped, total := pt.Progress()
	if completed != 2 {
		t.Errorf("expected completed=2, got %d", completed)
	}
	if skipped != 1 {
		t.Errorf("expected skipped=1, got %d", skipped)
	}
	if total != 10 {
		t.Errorf("expected total=10, got %d", total)
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

	pt := NewProgressTracker("test_phase", 10, log)

	// Record completions with known duration
	pt.RecordCompletion(100 * time.Millisecond)
	pt.RecordCompletion(100 * time.Millisecond)

	eta := pt.ETA()
	// With 2 completed at 100ms each, 8 remaining should be ~800ms
	if eta < 700*time.Millisecond || eta > 900*time.Millisecond {
		t.Errorf("expected ETA ~800ms, got %v", eta)
	}
}

func TestProgressTracker_ZeroTotal(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	pt := NewProgressTracker("test_phase", 0, log)

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
	SetPrettyMode(false)

	ce := NewCompletionEvent(log, "test_event", "test_phase", 500*time.Millisecond)
	ce.Str("key", "value").
		Int("count", 42).
		Int64("big_count", 1000000).
		Log("test message")

	output := buf.String()

	// Check required fields
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
	SetPrettyMode(true)

	ce := NewCompletionEvent(log, "test_event", "test_phase", 1*time.Second)
	ce.Bytes("size", 1073741824). // 1 GiB
					Count("items", 1500000).
					Log("test message")

	output := buf.String()

	// Check raw fields
	if !strings.Contains(output, `"size":1073741824`) {
		t.Errorf("expected raw size field, got: %s", output)
	}
	if !strings.Contains(output, `"items":1500000`) {
		t.Errorf("expected raw items field, got: %s", output)
	}

	// Check human-readable fields (pretty mode on)
	if !strings.Contains(output, `"size_h":"1.00 GiB"`) {
		t.Errorf("expected human size field, got: %s", output)
	}
	if !strings.Contains(output, `"items_h":"1.50M"`) {
		t.Errorf("expected human items field, got: %s", output)
	}

	SetPrettyMode(false)
}

func TestCompletionEvent_Progress(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	SetPrettyMode(true)

	ce := NewCompletionEvent(log, "test_event", "test_phase", 1*time.Second)
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

	SetPrettyMode(false)
}

func TestCompletionEvent_ProgressFromTracker(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	SetPrettyMode(false)

	pt := NewProgressTracker("test_phase", 100, log)
	pt.RecordCompletion(100 * time.Millisecond)
	pt.RecordCompletion(100 * time.Millisecond)
	pt.RecordSkip()

	ce := NewCompletionEvent(log, "test_event", "test_phase", 1*time.Second)
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
	SetPrettyMode(true)

	ce := NewCompletionEvent(log, "test_event", "test_phase", 1*time.Second)
	ce.Throughput(104857600). // 100 MiB in 1 second = 100 MiB/s
					Log("test message")

	output := buf.String()

	if !strings.Contains(output, `"throughput_bps":`) {
		t.Errorf("expected throughput_bps field, got: %s", output)
	}
	if !strings.Contains(output, `"throughput_h":"100.00 MiB/s"`) {
		t.Errorf("expected throughput_h field, got: %s", output)
	}

	SetPrettyMode(false)
}

func TestHelperFunctions(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	SetPrettyMode(false)

	// Test PhaseComplete
	PhaseComplete(log, "test_phase", 1*time.Second).
		Str("key", "value").
		Log("phase done")

	output := buf.String()
	if !strings.Contains(output, `"event":"phase_completed"`) {
		t.Errorf("expected phase_completed event, got: %s", output)
	}

	// Test ChunkComplete
	buf.Reset()
	ChunkComplete(log, "test_phase", 500*time.Millisecond).
		Str("chunk_id", "chunk1").
		Log("chunk done")

	output = buf.String()
	if !strings.Contains(output, `"event":"chunk_completed"`) {
		t.Errorf("expected chunk_completed event, got: %s", output)
	}

	// Test BatchComplete
	buf.Reset()
	BatchComplete(log, "test_phase", 200*time.Millisecond).
		Int("batch_size", 1000).
		Log("batch done")

	output = buf.String()
	if !strings.Contains(output, `"event":"batch_completed"`) {
		t.Errorf("expected batch_completed event, got: %s", output)
	}

	// Test FileCreated
	buf.Reset()
	FileCreated(log, "test_phase", 100*time.Millisecond).
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
	SetPrettyMode(false)

	// Temporarily lower global level to allow debug output
	oldLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	defer zerolog.SetGlobalLevel(oldLevel)

	ce := NewCompletionEvent(log, "test_event", "test_phase", 1*time.Second)
	ce.LogDebug("debug message")

	output := buf.String()
	if !strings.Contains(output, `"level":"debug"`) {
		t.Errorf("expected debug level, got: %s", output)
	}
}

func TestChunkStarted(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	SetPrettyMode(false)

	ChunkStarted(log, "aggregate", "chunk-001", 5, 100)

	output := buf.String()

	// Check required fields
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
	// chunk_started should NOT have progress_pct
	if strings.Contains(output, `"progress_pct"`) {
		t.Errorf("chunk_started should not have progress_pct, got: %s", output)
	}
}

func TestProgressTracker_MultipleChunks(t *testing.T) {
	var buf bytes.Buffer
	log := zerolog.New(&buf)

	pt := NewProgressTracker("test_phase", 10, log)

	// Simulate processing multiple chunks
	// Skip 2 chunks (already done)
	pt.RecordSkip()
	pt.RecordSkip()

	// Process 3 chunks
	pt.RecordCompletion(100 * time.Millisecond)
	pt.RecordCompletion(120 * time.Millisecond)
	pt.RecordCompletion(80 * time.Millisecond)

	// Verify progress computation
	completed, skipped, total := pt.Progress()
	if completed != 3 {
		t.Errorf("expected completed=3, got %d", completed)
	}
	if skipped != 2 {
		t.Errorf("expected skipped=2, got %d", skipped)
	}
	if total != 10 {
		t.Errorf("expected total=10, got %d", total)
	}

	// Progress should be (3 completed + 2 skipped) / 10 = 50%
	pct := pt.ProgressPct()
	if pct != 50.0 {
		t.Errorf("expected progress 50%%, got %.1f%%", pct)
	}

	// Remaining should be 5
	remaining := pt.Remaining()
	if remaining != 5 {
		t.Errorf("expected remaining=5, got %d", remaining)
	}

	// Completed() should return just the completed count
	if pt.Completed() != 3 {
		t.Errorf("expected Completed()=3, got %d", pt.Completed())
	}

	// Total() should return the total
	if pt.Total() != 10 {
		t.Errorf("expected Total()=10, got %d", pt.Total())
	}
}

func TestProgressTracker_ChunkCompletedAfterCommit(t *testing.T) {
	// This test verifies the pattern: only call RecordCompletion after commit
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	SetPrettyMode(false)

	pt := NewProgressTracker("aggregate", 3, log)

	// Simulate chunk 1: started -> processing -> committed
	// (chunk_started is logged separately, not via ProgressTracker)
	time.Sleep(10 * time.Millisecond) // simulate work
	pt.RecordCompletion(10 * time.Millisecond)

	// After first chunk: 1/3 = 33.33%
	pct := pt.ProgressPct()
	if pct < 33.0 || pct > 34.0 {
		t.Errorf("expected ~33%% after 1 chunk, got %.2f%%", pct)
	}

	// Simulate chunk 2
	time.Sleep(10 * time.Millisecond)
	pt.RecordCompletion(10 * time.Millisecond)

	// After second chunk: 2/3 = 66.67%
	pct = pt.ProgressPct()
	if pct < 66.0 || pct > 67.0 {
		t.Errorf("expected ~67%% after 2 chunks, got %.2f%%", pct)
	}

	// Simulate chunk 3
	time.Sleep(10 * time.Millisecond)
	pt.RecordCompletion(10 * time.Millisecond)

	// After third chunk: 3/3 = 100%
	pct = pt.ProgressPct()
	if pct != 100.0 {
		t.Errorf("expected 100%% after all chunks, got %.2f%%", pct)
	}
}

func TestCompletionEvent_OnlyAfterCommit(t *testing.T) {
	// Verify that chunk_completed logs include correct fields
	var buf bytes.Buffer
	log := zerolog.New(&buf)
	SetPrettyMode(false)

	pt := NewProgressTracker("aggregate", 10, log)
	pt.RecordCompletion(500 * time.Millisecond)

	// Create chunk_completed event with tracker data
	ce := ChunkComplete(log, "aggregate", 500*time.Millisecond)
	ce.Str("chunk_id", "test-chunk").
		Count("objects", 50000).
		Bytes("bytes", 1073741824).
		ProgressFromTracker(pt).
		Throughput(1073741824).
		Log("chunk committed to SQLite")

	output := buf.String()

	// Required fields for chunk_completed
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
