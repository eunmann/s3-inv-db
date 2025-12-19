package logging

import (
	"bytes"
	"testing"

	"github.com/rs/zerolog"
)

func TestInit_DoesNotPanic(t *testing.T) {
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
