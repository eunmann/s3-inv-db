// Package logging provides structured logging for s3-inv-db using zerolog.
//
// The package wraps zerolog's standard global logger (rs/zerolog/log) so it
// owns no mutable package-level state of its own: Init/SetLogger mutate
// zerologlog.Logger, and L() returns a pointer to it. Human-readable
// formatting ("pretty mode") is a per-builder flag, not a process-wide
// switch — see Operation, LogEvent, and CompletionEvent.
package logging

import (
	"os"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/humanfmt"
	"github.com/rs/zerolog"
	zerologlog "github.com/rs/zerolog/log"
)

// configureZerolog applies the project-wide zerolog defaults. The
// assignments are idempotent: TimeFieldFormat, DurationFieldFormat,
// and the global level are pure config knobs, and zerolog/log.Logger
// is rebuilt deterministically from the same writer. Called by every
// entry point so the first caller wins without requiring an init()
// function or a sync.Once sentinel.
func configureZerolog() {
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.DurationFieldFormat = zerolog.DurationFormatString
}

// Init configures the global logger.
// If debug is true, sets log level to Debug.
// If human is true, uses a human-friendly console writer.
func Init(debug, human bool) {
	configureZerolog()
	zerologlog.Logger = NewLogger(debug, human)
}

// NewLogger builds a configured zerolog.Logger for callers that prefer
// passing a logger explicitly (e.g. main funcs, HTTP server). It also
// sets zerolog's global level so package-level zerolog calls share the
// same threshold.
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

// L returns the base logger. Routes through zerolog's standard log
// package so the logging package itself owns no mutable global state.
func L() *zerolog.Logger {
	configureZerolog()

	return &zerologlog.Logger
}

// WithPhase returns a logger with the phase field set.
func WithPhase(phase string) zerolog.Logger {
	return L().With().Str("phase", phase).Logger()
}

// SetLogger allows overriding the global logger (useful for testing).
func SetLogger(l zerolog.Logger) {
	configureZerolog()
	zerologlog.Logger = l
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
	pretty bool
	fields map[string]any
}

// NewOperation creates a new operation timer with the given name.
func NewOperation(name string) *Operation {
	return &Operation{
		name:   name,
		start:  time.Now(),
		log:    *L(),
		fields: make(map[string]any),
	}
}

// NewOperationWithLogger creates a new operation timer with a specific logger.
func NewOperationWithLogger(name string, log zerolog.Logger) *Operation {
	return &Operation{
		name:   name,
		start:  time.Now(),
		log:    log,
		fields: make(map[string]any),
	}
}

// Pretty toggles human-readable companion fields (e.g. *_h) on
// subsequent WithBytes/WithCount/End calls. Returns the operation for
// chaining.
func (o *Operation) Pretty(on bool) *Operation {
	o.pretty = on

	return o
}

// WithField adds a field to be logged when the operation ends.
func (o *Operation) WithField(key string, value any) *Operation {
	o.fields[key] = value

	return o
}

// WithBytes adds byte count fields (raw + human-readable when pretty).
func (o *Operation) WithBytes(key string, bytes int64) *Operation {
	o.fields[key] = bytes
	if o.pretty {
		o.fields[key+"_h"] = humanfmt.Bytes(bytes)
	}

	return o
}

// WithBytesUint64 adds byte count fields for uint64.
func (o *Operation) WithBytesUint64(key string, bytes uint64) *Operation {
	return o.WithBytes(key, int64(bytes))
}

// WithCount adds count fields (raw + human-readable when pretty).
func (o *Operation) WithCount(key string, count int64) *Operation {
	o.fields[key] = count
	if o.pretty {
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

	if o.pretty {
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

	if o.pretty {
		event = event.
			Str("elapsed_h", humanfmt.Duration(elapsed)).
			Str("bytes_h", humanfmt.Bytes(bytes)).
			Str("throughput_h", humanfmt.Throughput(bytes, elapsed))
	}

	// Throughput in bytes/sec for machine parsing.
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

	if o.pretty {
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

	if o.pretty {
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

// LogEvent is a helper for building log events with human-readable
// fields. It buffers field additions in a map and constructs the
// underlying *zerolog.Event lazily at Msg() time so the event never
// escapes through a struct field — zerologlint can't track an event
// stored in a struct, and this design keeps it satisfied.
type LogEvent struct {
	log    zerolog.Logger
	level  zerolog.Level
	pretty bool
	fields map[string]any
}

// InfoEvent starts a new info-level log event helper. The pretty flag
// controls whether human-readable companion fields (e.g. *_h) are
// emitted alongside the raw fields.
func InfoEvent(log zerolog.Logger, pretty bool) *LogEvent {
	return newLogEvent(log, zerolog.InfoLevel, pretty)
}

// DebugEvent starts a new debug-level log event helper.
func DebugEvent(log zerolog.Logger, pretty bool) *LogEvent {
	return newLogEvent(log, zerolog.DebugLevel, pretty)
}

func newLogEvent(log zerolog.Logger, level zerolog.Level, pretty bool) *LogEvent {
	return &LogEvent{
		log:    log,
		level:  level,
		pretty: pretty,
		fields: make(map[string]any),
	}
}

// Bytes adds a byte count field with optional human-readable companion.
func (le *LogEvent) Bytes(key string, bytes int64) *LogEvent {
	le.fields[key] = bytes
	if le.pretty {
		le.fields[key+"_h"] = humanfmt.Bytes(bytes)
	}

	return le
}

// BytesUint64 adds a uint64 byte count field.
func (le *LogEvent) BytesUint64(key string, bytes uint64) *LogEvent {
	return le.Bytes(key, int64(bytes))
}

// Duration adds a duration field with optional human-readable companion.
func (le *LogEvent) Duration(key string, d time.Duration) *LogEvent {
	le.fields[key] = d
	if le.pretty {
		le.fields[key+"_h"] = humanfmt.Duration(d)
	}

	return le
}

// Count adds a count field with optional human-readable companion.
func (le *LogEvent) Count(key string, n int64) *LogEvent {
	le.fields[key] = n
	if le.pretty {
		le.fields[key+"_h"] = humanfmt.Count(n)
	}

	return le
}

// CountUint64 adds a uint64 count field.
func (le *LogEvent) CountUint64(key string, n uint64) *LogEvent {
	return le.Count(key, int64(n))
}

// Throughput adds throughput fields (raw bps + human-readable when pretty).
func (le *LogEvent) Throughput(key string, bytes int64, d time.Duration) *LogEvent {
	if d > 0 {
		bps := float64(bytes) / d.Seconds()
		le.fields[key+"_bps"] = bps
		if le.pretty {
			le.fields[key+"_h"] = humanfmt.Throughput(bytes, d)
		}
	}

	return le
}

// Str adds a string field.
func (le *LogEvent) Str(key, val string) *LogEvent {
	le.fields[key] = val

	return le
}

// Int adds an int field.
func (le *LogEvent) Int(key string, val int) *LogEvent {
	le.fields[key] = val

	return le
}

// Int64 adds an int64 field.
func (le *LogEvent) Int64(key string, val int64) *LogEvent {
	le.fields[key] = val

	return le
}

// Uint64 adds a uint64 field.
func (le *LogEvent) Uint64(key string, val uint64) *LogEvent {
	le.fields[key] = val

	return le
}

// Msg builds and dispatches the log event with the given message. The
// underlying *zerolog.Event is created and Msg'd in the same function
// body so the event never escapes — zerologlint accepts the chain.
func (le *LogEvent) Msg(msg string) {
	e := le.log.WithLevel(le.level)
	for k, v := range le.fields {
		e = e.Interface(k, v)
	}
	e.Msg(msg)
}
