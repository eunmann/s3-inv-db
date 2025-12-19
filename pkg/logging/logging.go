// Package logging provides structured logging for s3inv-index using zerolog.
package logging

import (
	"os"
	"sync/atomic"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/rs/zerolog"
)

var (
	logger     *zerolog.Logger
	prettyMode atomic.Bool
)

func init() {
	// Default to JSON logging at info level
	l := zerolog.New(os.Stderr).With().Timestamp().Logger()
	logger = &l
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

// Init configures the global logger.
// If debug is true, sets log level to Debug.
// If human is true, uses a human-friendly console writer and enables pretty mode.
func Init(debug bool, human bool) {
	level := zerolog.InfoLevel
	if debug {
		level = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(level)
	prettyMode.Store(human)

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

// IsPrettyMode returns true if human-readable formatting is enabled.
func IsPrettyMode() bool {
	return prettyMode.Load()
}

// SetPrettyMode enables or disables human-readable field formatting.
func SetPrettyMode(enabled bool) {
	prettyMode.Store(enabled)
}

// Operation tracks timing for a named operation.
// Usage:
//
//	op := logging.NewOperation("chunk_download")
//	defer op.End()
//	// ... do work ...
//	op.EndWithBytes(bytesProcessed) // or just op.End()
type Operation struct {
	name   string
	start  time.Time
	log    zerolog.Logger
	fields map[string]interface{}
}

// NewOperation creates a new operation timer with the given name.
func NewOperation(name string) *Operation {
	return &Operation{
		name:   name,
		start:  time.Now(),
		log:    *logger,
		fields: make(map[string]interface{}),
	}
}

// NewOperationWithLogger creates a new operation timer with a specific logger.
func NewOperationWithLogger(name string, log zerolog.Logger) *Operation {
	return &Operation{
		name:   name,
		start:  time.Now(),
		log:    log,
		fields: make(map[string]interface{}),
	}
}

// WithField adds a field to be logged when the operation ends.
func (o *Operation) WithField(key string, value interface{}) *Operation {
	o.fields[key] = value
	return o
}

// WithBytes adds byte count fields (raw + human-readable if pretty mode).
func (o *Operation) WithBytes(key string, bytes int64) *Operation {
	o.fields[key] = bytes
	if IsPrettyMode() {
		o.fields[key+"_h"] = humanfmt.Bytes(bytes)
	}
	return o
}

// WithBytesUint64 adds byte count fields for uint64.
func (o *Operation) WithBytesUint64(key string, bytes uint64) *Operation {
	return o.WithBytes(key, int64(bytes))
}

// WithCount adds count fields (raw + human-readable if pretty mode).
func (o *Operation) WithCount(key string, count int64) *Operation {
	o.fields[key] = count
	if IsPrettyMode() {
		o.fields[key+"_h"] = humanfmt.Count(count)
	}
	return o
}

// Elapsed returns the time since the operation started.
func (o *Operation) Elapsed() time.Duration {
	return time.Since(o.start)
}

// End logs the operation completion with duration.
func (o *Operation) End() {
	elapsed := o.Elapsed()
	event := o.log.Info().
		Str("operation", o.name).
		Dur("elapsed_ms", elapsed)

	if IsPrettyMode() {
		event = event.Str("elapsed_h", humanfmt.Duration(elapsed))
	}

	for k, v := range o.fields {
		event = event.Interface(k, v)
	}

	event.Msg("operation complete")
}

// EndWithBytes logs operation completion with bytes processed and throughput.
func (o *Operation) EndWithBytes(bytes int64) {
	elapsed := o.Elapsed()
	event := o.log.Info().
		Str("operation", o.name).
		Dur("elapsed_ms", elapsed).
		Int64("bytes", bytes)

	if IsPrettyMode() {
		event = event.
			Str("elapsed_h", humanfmt.Duration(elapsed)).
			Str("bytes_h", humanfmt.Bytes(bytes)).
			Str("throughput_h", humanfmt.Throughput(bytes, elapsed))
	}

	// Add throughput in bytes/sec for machine parsing
	if elapsed > 0 {
		throughput := float64(bytes) / elapsed.Seconds()
		event = event.Float64("throughput_bps", throughput)
	}

	for k, v := range o.fields {
		event = event.Interface(k, v)
	}

	event.Msg("operation complete")
}

// EndDebug logs the operation at debug level.
func (o *Operation) EndDebug() {
	elapsed := o.Elapsed()
	event := o.log.Debug().
		Str("operation", o.name).
		Dur("elapsed_ms", elapsed)

	if IsPrettyMode() {
		event = event.Str("elapsed_h", humanfmt.Duration(elapsed))
	}

	for k, v := range o.fields {
		event = event.Interface(k, v)
	}

	event.Msg("operation complete")
}

// EndWithBytesDebug logs operation completion at debug level.
func (o *Operation) EndWithBytesDebug(bytes int64) {
	elapsed := o.Elapsed()
	event := o.log.Debug().
		Str("operation", o.name).
		Dur("elapsed_ms", elapsed).
		Int64("bytes", bytes)

	if IsPrettyMode() {
		event = event.
			Str("elapsed_h", humanfmt.Duration(elapsed)).
			Str("bytes_h", humanfmt.Bytes(bytes)).
			Str("throughput_h", humanfmt.Throughput(bytes, elapsed))
	}

	if elapsed > 0 {
		throughput := float64(bytes) / elapsed.Seconds()
		event = event.Float64("throughput_bps", throughput)
	}

	for k, v := range o.fields {
		event = event.Interface(k, v)
	}

	event.Msg("operation complete")
}

// LogEvent is a helper for building log events with human-readable fields.
type LogEvent struct {
	event *zerolog.Event
}

// Event creates a new log event helper from a zerolog event.
func Event(e *zerolog.Event) *LogEvent {
	return &LogEvent{event: e}
}

// Bytes adds a byte count field with optional human-readable companion.
func (le *LogEvent) Bytes(key string, bytes int64) *LogEvent {
	le.event = le.event.Int64(key, bytes)
	if IsPrettyMode() {
		le.event = le.event.Str(key+"_h", humanfmt.Bytes(bytes))
	}
	return le
}

// BytesUint64 adds a uint64 byte count field.
func (le *LogEvent) BytesUint64(key string, bytes uint64) *LogEvent {
	return le.Bytes(key, int64(bytes))
}

// Duration adds a duration field with optional human-readable companion.
func (le *LogEvent) Duration(key string, d time.Duration) *LogEvent {
	le.event = le.event.Dur(key, d)
	if IsPrettyMode() {
		le.event = le.event.Str(key+"_h", humanfmt.Duration(d))
	}
	return le
}

// Count adds a count field with optional human-readable companion.
func (le *LogEvent) Count(key string, n int64) *LogEvent {
	le.event = le.event.Int64(key, n)
	if IsPrettyMode() {
		le.event = le.event.Str(key+"_h", humanfmt.Count(n))
	}
	return le
}

// CountUint64 adds a uint64 count field.
func (le *LogEvent) CountUint64(key string, n uint64) *LogEvent {
	return le.Count(key, int64(n))
}

// Throughput adds throughput fields (raw bps + human-readable if pretty).
func (le *LogEvent) Throughput(key string, bytes int64, d time.Duration) *LogEvent {
	if d > 0 {
		bps := float64(bytes) / d.Seconds()
		le.event = le.event.Float64(key+"_bps", bps)
		if IsPrettyMode() {
			le.event = le.event.Str(key+"_h", humanfmt.Throughput(bytes, d))
		}
	}
	return le
}

// Str adds a string field.
func (le *LogEvent) Str(key, val string) *LogEvent {
	le.event = le.event.Str(key, val)
	return le
}

// Int adds an int field.
func (le *LogEvent) Int(key string, val int) *LogEvent {
	le.event = le.event.Int(key, val)
	return le
}

// Int64 adds an int64 field.
func (le *LogEvent) Int64(key string, val int64) *LogEvent {
	le.event = le.event.Int64(key, val)
	return le
}

// Uint64 adds a uint64 field.
func (le *LogEvent) Uint64(key string, val uint64) *LogEvent {
	le.event = le.event.Uint64(key, val)
	return le
}

// Msg sends the log event with a message.
func (le *LogEvent) Msg(msg string) {
	le.event.Msg(msg)
}
