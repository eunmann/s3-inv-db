// Package logctx provides context-based logger injection and extraction.
//
// This package enables passing loggers through context.Context, allowing
// callers to inject enriched loggers with contextual fields (e.g., chunk_index,
// run_id) that automatically propagate through the call stack.
//
// Usage:
//
//	// At the top level, create a context with the base logger:
//	ctx := logctx.WithLogger(ctx, baseLogger)
//
//	// In functions, extract the logger:
//	logger := logctx.FromContext(ctx)
//
//	// To add contextual fields for a sub-operation:
//	childLogger := logctx.FromContext(ctx).With().Int("chunk_index", i).Logger()
//	childCtx := logctx.WithLogger(ctx, childLogger)
package logctx

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// loggerKey is the private key type for storing loggers in context.
// Using a private type prevents collisions with other packages.
type loggerKey struct{}

var (
	defaultLogger     zerolog.Logger
	defaultLoggerOnce sync.Once
)

// initDefaultLogger initializes the default logger once.
func initDefaultLogger() {
	defaultLoggerOnce.Do(func() {
		defaultLogger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	})
}

// DefaultLogger returns the process-wide default logger used when no
// context logger is available. This logger outputs JSON to stderr with
// timestamps.
func DefaultLogger() zerolog.Logger {
	initDefaultLogger()
	return defaultLogger
}

// SetDefaultLogger overrides the default logger. This should only be called
// during initialization (e.g., from main or init). It is not safe to call
// concurrently with FromContext.
func SetDefaultLogger(l zerolog.Logger) {
	initDefaultLogger() // Ensure once is done
	defaultLogger = l
}

// WithLogger returns a new context with the given logger attached.
// The logger can be retrieved using FromContext.
func WithLogger(ctx context.Context, logger zerolog.Logger) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, loggerKey{}, logger)
}

// FromContext extracts the logger from the context. If the context is nil
// or does not contain a logger, returns the default logger.
//
// This function never returns a zero-value logger or panics.
func FromContext(ctx context.Context) zerolog.Logger {
	if ctx == nil {
		return DefaultLogger()
	}
	if logger, ok := ctx.Value(loggerKey{}).(zerolog.Logger); ok {
		return logger
	}
	return DefaultLogger()
}

// WithField returns a new context with a logger that has the specified field added.
// This is a convenience function that combines FromContext, With, and WithLogger.
func WithField(ctx context.Context, key string, value interface{}) context.Context {
	logger := FromContext(ctx).With().Interface(key, value).Logger()
	return WithLogger(ctx, logger)
}

// WithStr returns a new context with a logger that has the specified string field added.
func WithStr(ctx context.Context, key, value string) context.Context {
	logger := FromContext(ctx).With().Str(key, value).Logger()
	return WithLogger(ctx, logger)
}

// WithInt returns a new context with a logger that has the specified int field added.
func WithInt(ctx context.Context, key string, value int) context.Context {
	logger := FromContext(ctx).With().Int(key, value).Logger()
	return WithLogger(ctx, logger)
}

// NewConfiguredLogger creates a new logger with the specified configuration.
// If debug is true, sets log level to Debug.
// If human is true, uses a human-friendly console writer.
func NewConfiguredLogger(debug, human bool) zerolog.Logger {
	level := zerolog.InfoLevel
	if debug {
		level = zerolog.DebugLevel
	}

	var output zerolog.LevelWriter
	if human {
		output = zerolog.LevelWriterAdapter{Writer: zerolog.ConsoleWriter{
			Out:        os.Stderr,
			TimeFormat: time.RFC3339,
			NoColor:    false,
		}}
	} else {
		output = zerolog.LevelWriterAdapter{Writer: os.Stderr}
	}

	return zerolog.New(output).Level(level).With().Timestamp().Logger()
}
