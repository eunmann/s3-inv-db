// Package logging provides the project-wide zerolog defaults and
// initialisation. The package wraps zerolog's standard global logger
// so it owns no mutable state of its own.
package logging

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
)

// configureZerolog applies the project-wide zerolog defaults.
// Idempotent — every entry point calls it so the first caller wins.
func configureZerolog() {
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.DurationFieldFormat = zerolog.DurationFormatString
}

// Init configures the global logger. Human=true selects a console
// writer; debug=true lowers the global level to Debug.
func Init(debug, human bool) {
	configureZerolog()
	zerologlog.Logger = NewLogger(debug, human)
}

// NewLogger builds a configured zerolog.Logger for callers that prefer
// passing a logger explicitly. Also sets zerolog's global level so
// package-level zerolog calls share the same threshold.
func NewLogger(debug, human bool) zerolog.Logger {
	configureZerolog()
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

	return zerolog.New(output).With().Timestamp().Logger()
}

func L() *zerolog.Logger {
	configureZerolog()

	return &zerologlog.Logger
}
