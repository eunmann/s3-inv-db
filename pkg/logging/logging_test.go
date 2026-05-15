package logging_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/rs/zerolog"
)

func TestInit_DoesNotPanic(_ *testing.T) {
	// Test JSON mode (default).
	logging.Init(false, false)
	log := logging.L()
	log.Info().Msg("test json info")
	log.Debug().Msg("test json debug (should not appear at info level)")

	// Test debug mode.
	logging.Init(true, false)
	log = logging.L()
	log.Debug().Msg("test json debug (should appear)")

	// Test human-friendly mode.
	logging.Init(false, true)
	log = logging.L()
	log.Info().Msg("test human info")

	// Test debug + human.
	logging.Init(true, true)
	log = logging.L()
	log.Debug().Msg("test human debug")
}

func TestLogEvent_PrettyMode(t *testing.T) {
	tests := []struct {
		name      string
		pretty    bool
		wantHuman bool
	}{
		{"pretty_off", false, false},
		{"pretty_on", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			log := zerolog.New(&buf)

			logging.InfoEvent(log, tt.pretty).
				Bytes("bytes", 1073741824).
				Duration("elapsed", 1500*time.Millisecond).
				Count("count", 1500000).
				Msg("test")

			output := buf.String()

			// Raw fields should always be present.
			if !bytes.Contains(buf.Bytes(), []byte(`"bytes":1073741824`)) {
				t.Errorf("expected raw bytes field, got: %s", output)
			}
			if !bytes.Contains(buf.Bytes(), []byte(`"count":1500000`)) {
				t.Errorf("expected raw count field, got: %s", output)
			}

			// Human fields depend on mode.
			hasHumanBytes := bytes.Contains(buf.Bytes(), []byte(`"bytes_h":"1.00 GiB"`))
			hasHumanCount := bytes.Contains(buf.Bytes(), []byte(`"count_h":"1.5M"`))
			hasHumanElapsed := bytes.Contains(buf.Bytes(), []byte(`"elapsed_h":`))

			if tt.wantHuman {
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

	logging.Init(false, false)
}

func TestOperation(t *testing.T) {
	var buf bytes.Buffer
	logging.SetLogger(zerolog.New(&buf))

	op := logging.NewOperation("test_op")
	time.Sleep(10 * time.Millisecond)
	op.End()

	output := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte(`"operation":"test_op"`)) {
		t.Errorf("expected operation name, got: %s", output)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"elapsed_ms":`)) {
		t.Errorf("expected elapsed_ms field, got: %s", output)
	}

	logging.Init(false, false)
}

func TestOperation_WithBytes(t *testing.T) {
	var buf bytes.Buffer
	logging.SetLogger(zerolog.New(&buf))

	op := logging.NewOperation("byte_op").Pretty(true)
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

	logging.Init(false, false)
}

func TestOperation_WithFields(t *testing.T) {
	var buf bytes.Buffer
	logging.SetLogger(zerolog.New(&buf))

	op := logging.NewOperation("field_op").Pretty(true)
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
	if !bytes.Contains(buf.Bytes(), []byte(`"items_h":"1.5M"`)) {
		t.Errorf("expected items_h field, got: %s", output)
	}

	logging.Init(false, false)
}

func TestWithPhase(t *testing.T) {
	var buf bytes.Buffer
	logging.SetLogger(zerolog.New(&buf))

	log := logging.WithPhase("test_phase")
	log.Info().Msg("test message")

	output := buf.String()
	if output == "" {
		t.Error("expected log output, got empty string")
	}

	if !bytes.Contains(buf.Bytes(), []byte(`"phase":"test_phase"`)) {
		t.Errorf("expected phase field in output, got: %s", output)
	}
}

func TestSetLogger(t *testing.T) {
	var buf bytes.Buffer
	customLogger := zerolog.New(&buf).With().Str("custom", "field").Logger()
	logging.SetLogger(customLogger)

	logging.L().Info().Msg("test")

	if !bytes.Contains(buf.Bytes(), []byte(`"custom":"field"`)) {
		t.Errorf("expected custom field in output, got: %s", buf.String())
	}

	logging.Init(false, false)
}
