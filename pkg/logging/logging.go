// Package logging provides the project-wide zerolog defaults and
// initialisation. The package wraps zerolog's standard global logger
// so it owns no mutable state of its own.
package logging

import (
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
)

// configureOnce guards the one-time zerolog package-global mutation.
// Prior code mutated TimeFieldFormat / DurationFieldFormat from every
// L() call, which races concurrent reads from goroutines that already
// hold a logger reference.
var configureOnce sync.Once

// configureZerolog applies the project-wide zerolog defaults exactly
// once per process. Safe to call from any goroutine.
func configureZerolog() {
	configureOnce.Do(func() {
		zerolog.TimeFieldFormat = time.RFC3339
		zerolog.DurationFieldFormat = zerolog.DurationFormatString
	})
}

// Options configures the project-wide logger. Debug lowers the global
// level to Debug; Human selects the console (timestamp-prefixed,
// human-readable) writer instead of JSON.
type Options struct {
	Debug bool
	Human bool
}

// Init configures the global logger using opts.
func Init(opts Options) {
	configureZerolog()
	zerologlog.Logger = NewLogger(opts)
}

// NewLogger builds a configured zerolog.Logger for callers that prefer
// passing a logger explicitly. Also sets zerolog's global level so
// package-level zerolog calls share the same threshold.
func NewLogger(opts Options) zerolog.Logger {
	configureZerolog()
	level := zerolog.InfoLevel
	if opts.Debug {
		level = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(level)

	var output zerolog.LevelWriter
	if opts.Human {
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
