// Package logging provides structured logging for s3inv-index using zerolog.
package logging

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

var logger *zerolog.Logger

func init() {
	// Default to JSON logging at info level
	l := zerolog.New(os.Stderr).With().Timestamp().Logger()
	logger = &l
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

// Init configures the global logger.
// If debug is true, sets log level to Debug.
// If human is true, uses a human-friendly console writer.
func Init(debug bool, human bool) {
	level := zerolog.InfoLevel
	if debug {
		level = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(level)

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

	l := zerolog.New(output).With().Timestamp().Logger()
	logger = &l
}

// L returns the base logger.
func L() *zerolog.Logger {
	return logger
}

// WithPhase returns a logger with the phase field set.
func WithPhase(phase string) zerolog.Logger {
	return logger.With().Str("phase", phase).Logger()
}

// SetLogger allows overriding the global logger (useful for testing).
func SetLogger(l zerolog.Logger) {
	logger = &l
}
