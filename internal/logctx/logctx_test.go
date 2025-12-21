package logctx

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestFromContext_NilContext(t *testing.T) {
	// FromContext(nil) should return default logger, not panic
	logger := FromContext(nil)

	// Verify it works by logging something
	var buf bytes.Buffer
	testLogger := logger.Output(&buf)
	testLogger.Info().Msg("test")

	if buf.Len() == 0 {
		t.Error("expected logger to produce output")
	}
}

func TestFromContext_ContextWithoutLogger(t *testing.T) {
	ctx := context.Background()
	logger := FromContext(ctx)

	// Should return default logger
	var buf bytes.Buffer
	testLogger := logger.Output(&buf)
	testLogger.Info().Msg("test")

	if buf.Len() == 0 {
		t.Error("expected logger to produce output")
	}
}

func TestWithLogger_AndFromContext(t *testing.T) {
	var buf bytes.Buffer
	customLogger := zerolog.New(&buf).With().Str("custom", "field").Logger()

	ctx := WithLogger(context.Background(), customLogger)
	logger := FromContext(ctx)

	logger.Info().Msg("test")

	output := buf.String()
	if !strings.Contains(output, `"custom":"field"`) {
		t.Errorf("expected custom field in output, got: %s", output)
	}
}

func TestWithLogger_NilContext(t *testing.T) {
	var buf bytes.Buffer
	customLogger := zerolog.New(&buf)

	// Should not panic with nil context
	ctx := WithLogger(nil, customLogger)
	if ctx == nil {
		t.Error("expected non-nil context")
	}

	logger := FromContext(ctx)
	logger.Info().Msg("test")

	if buf.Len() == 0 {
		t.Error("expected logger to produce output")
	}
}

func TestWithField(t *testing.T) {
	var buf bytes.Buffer
	baseLogger := zerolog.New(&buf)
	ctx := WithLogger(context.Background(), baseLogger)

	ctx = WithField(ctx, "key", "value")
	logger := FromContext(ctx)
	logger.Info().Msg("test")

	output := buf.String()
	if !strings.Contains(output, `"key":"value"`) {
		t.Errorf("expected field in output, got: %s", output)
	}
}

func TestWithStr(t *testing.T) {
	var buf bytes.Buffer
	baseLogger := zerolog.New(&buf)
	ctx := WithLogger(context.Background(), baseLogger)

	ctx = WithStr(ctx, "phase", "ingest")
	logger := FromContext(ctx)
	logger.Info().Msg("test")

	output := buf.String()
	if !strings.Contains(output, `"phase":"ingest"`) {
		t.Errorf("expected phase field in output, got: %s", output)
	}
}

func TestWithInt(t *testing.T) {
	var buf bytes.Buffer
	baseLogger := zerolog.New(&buf)
	ctx := WithLogger(context.Background(), baseLogger)

	ctx = WithInt(ctx, "chunk_index", 42)
	logger := FromContext(ctx)
	logger.Info().Msg("test")

	output := buf.String()
	if !strings.Contains(output, `"chunk_index":42`) {
		t.Errorf("expected chunk_index field in output, got: %s", output)
	}
}

func TestDefaultLogger(t *testing.T) {
	logger := DefaultLogger()

	// Should produce output without panic
	var buf bytes.Buffer
	testLogger := logger.Output(&buf)
	testLogger.Info().Msg("test")

	if buf.Len() == 0 {
		t.Error("expected default logger to produce output")
	}
}

func TestNewConfiguredLogger(t *testing.T) {
	tests := []struct {
		name  string
		debug bool
		human bool
	}{
		{"json_info", false, false},
		{"json_debug", true, false},
		{"human_info", false, true},
		{"human_debug", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := NewConfiguredLogger(tt.debug, tt.human)

			// Verify logger works
			var buf bytes.Buffer
			testLogger := logger.Output(&buf)
			testLogger.Info().Msg("test info")
			testLogger.Debug().Msg("test debug")

			// Info should always appear
			if buf.Len() == 0 {
				t.Error("expected logger to produce output")
			}
		})
	}
}

func TestChainedContexts(t *testing.T) {
	var buf bytes.Buffer
	baseLogger := zerolog.New(&buf)

	ctx := WithLogger(context.Background(), baseLogger)
	ctx = WithStr(ctx, "phase", "ingest")
	ctx = WithInt(ctx, "chunk_index", 5)

	logger := FromContext(ctx)
	logger.Info().Msg("test")

	output := buf.String()
	if !strings.Contains(output, `"phase":"ingest"`) {
		t.Errorf("expected phase field, got: %s", output)
	}
	if !strings.Contains(output, `"chunk_index":5`) {
		t.Errorf("expected chunk_index field, got: %s", output)
	}
}
