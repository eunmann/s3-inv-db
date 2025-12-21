package logging

import (
	"bytes"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestInit_DoesNotPanic(_ *testing.T) {
	// Test JSON mode (default)
	Init(false, false)
	log := L()
	log.Info().Msg("test json info")
	log.Debug().Msg("test json debug (should not appear at info level)")

	// Test debug mode
	Init(true, false)
	log = L()
	log.Debug().Msg("test json debug (should appear)")

	// Test human-friendly mode
	Init(false, true)
	log = L()
	log.Info().Msg("test human info")

	// Test debug + human
	Init(true, true)
	log = L()
	log.Debug().Msg("test human debug")
}

func TestPrettyMode(t *testing.T) {
	// Default should be false
	SetPrettyMode(false)
	if IsPrettyMode() {
		t.Error("expected pretty mode to be false by default")
	}

	// Set to true
	SetPrettyMode(true)
	if !IsPrettyMode() {
		t.Error("expected pretty mode to be true after SetPrettyMode(true)")
	}

	// Reset
	SetPrettyMode(false)
	if IsPrettyMode() {
		t.Error("expected pretty mode to be false after SetPrettyMode(false)")
	}
}

func TestLogEvent_PrettyMode(t *testing.T) {
	tests := []struct {
		name       string
		prettyMode bool
		wantHuman  bool
	}{
		{"pretty_off", false, false},
		{"pretty_on", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			SetLogger(zerolog.New(&buf))
			SetPrettyMode(tt.prettyMode)

			//nolint:zerologlint // Event() wrapper terminates with Msg()
			Event(L().Info()).
				Bytes("bytes", 1073741824).
				Duration("elapsed", 1500*time.Millisecond).
				Count("count", 1500000).
				Msg("test")

			output := buf.String()

			// Raw fields should always be present
			if !bytes.Contains(buf.Bytes(), []byte(`"bytes":1073741824`)) {
				t.Errorf("expected raw bytes field, got: %s", output)
			}
			if !bytes.Contains(buf.Bytes(), []byte(`"count":1500000`)) {
				t.Errorf("expected raw count field, got: %s", output)
			}

			// Human fields depend on mode
			hasHumanBytes := bytes.Contains(buf.Bytes(), []byte(`"bytes_h":"1.00 GiB"`))
			hasHumanCount := bytes.Contains(buf.Bytes(), []byte(`"count_h":"1.50M"`))
			hasHumanElapsed := bytes.Contains(buf.Bytes(), []byte(`"elapsed_h":`))

			if tt.wantHuman { //nolint:nestif // Test validation branches
				if !hasHumanBytes {
					t.Errorf("expected human bytes field in pretty mode, got: %s", output)
				}
				if !hasHumanCount {
					t.Errorf("expected human count field in pretty mode, got: %s", output)
				}
				if !hasHumanElapsed {
					t.Errorf("expected human elapsed field in pretty mode, got: %s", output)
				}
			} else {
				if hasHumanBytes {
					t.Errorf("unexpected human bytes field when not in pretty mode, got: %s", output)
				}
				if hasHumanCount {
					t.Errorf("unexpected human count field when not in pretty mode, got: %s", output)
				}
				if hasHumanElapsed {
					t.Errorf("unexpected human elapsed field when not in pretty mode, got: %s", output)
				}
			}
		})
	}

	// Reset
	SetPrettyMode(false)
	Init(false, false)
}

func TestOperation(t *testing.T) {
	var buf bytes.Buffer
	SetLogger(zerolog.New(&buf))
	SetPrettyMode(false)

	// Test basic operation
	op := NewOperation("test_op")
	time.Sleep(10 * time.Millisecond)
	op.End()

	output := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte(`"operation":"test_op"`)) {
		t.Errorf("expected operation name, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"elapsed_ms":`)) {
		t.Errorf("expected elapsed_ms field, got: %s", output)
	}

	// Reset
	SetPrettyMode(false)
	Init(false, false)
}

func TestOperation_WithBytes(t *testing.T) {
	var buf bytes.Buffer
	SetLogger(zerolog.New(&buf))
	SetPrettyMode(true)

	op := NewOperation("byte_op")
	op.EndWithBytes(104857600) // 100 MiB

	output := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte(`"bytes":104857600`)) {
		t.Errorf("expected raw bytes, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"bytes_h":"100.00 MiB"`)) {
		t.Errorf("expected human bytes, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"throughput_h":`)) {
		t.Errorf("expected throughput_h, got: %s", output)
	}

	// Reset
	SetPrettyMode(false)
	Init(false, false)
}

func TestOperation_WithFields(t *testing.T) {
	var buf bytes.Buffer
	SetLogger(zerolog.New(&buf))
	SetPrettyMode(true)

	op := NewOperation("field_op")
	op.WithField("custom", "value").
		WithBytes("data_size", 1048576).
		WithCount("items", 1500000)
	op.End()

	output := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte(`"custom":"value"`)) {
		t.Errorf("expected custom field, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"data_size":1048576`)) {
		t.Errorf("expected data_size field, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"data_size_h":"1.00 MiB"`)) {
		t.Errorf("expected data_size_h field, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"items":1500000`)) {
		t.Errorf("expected items field, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"items_h":"1.50M"`)) {
		t.Errorf("expected items_h field, got: %s", output)
	}

	// Reset
	SetPrettyMode(false)
	Init(false, false)
}

func TestWithPhase(t *testing.T) {
	var buf bytes.Buffer
	SetLogger(zerolog.New(&buf))

	log := WithPhase("test_phase")
	log.Info().Msg("test message")

	output := buf.String()
	if output == "" {
		t.Error("expected log output, got empty string")
	}

	// Check that phase field is present
	if !bytes.Contains(buf.Bytes(), []byte(`"phase":"test_phase"`)) {
		t.Errorf("expected phase field in output, got: %s", output)
	}
}

func TestSetLogger(t *testing.T) {
	var buf bytes.Buffer
	customLogger := zerolog.New(&buf).With().Str("custom", "field").Logger()
	SetLogger(customLogger)

	L().Info().Msg("test")

	if !bytes.Contains(buf.Bytes(), []byte(`"custom":"field"`)) {
		t.Errorf("expected custom field in output, got: %s", buf.String())
	}

	// Reset to default for other tests
	Init(false, false)
}
