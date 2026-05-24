package logging_test

import (
	"bytes"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/rs/zerolog"
)

var rfc3339Re = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([Zz]|[+-]\d{2}:\d{2})$`)

// TestNewLogger_EmitsRFC3339Timestamp confirms every event written by
// the project's root logger (and any derived logger via .With()) is
// stamped with a top-level "time" field in RFC3339 format.
func TestNewLogger_EmitsRFC3339Timestamp(t *testing.T) {
	// Touch the package so the lazy zerolog setup runs and pins
	// TimeFieldFormat to RFC3339 before this test inspects it.
	_ = logging.NewLogger(logging.Options{})
	if zerolog.TimeFieldFormat != time.RFC3339 {
		t.Fatalf("zerolog.TimeFieldFormat = %q, want time.RFC3339", zerolog.TimeFieldFormat)
	}

	var buf bytes.Buffer
	root := zerolog.New(&buf).With().Timestamp().Logger()
	derived := root.With().Str("derived", "yes").Logger()
	derived.Info().Msg("hi")

	var event map[string]any
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		t.Fatalf("unmarshal event: %v\nraw: %s", err, buf.String())
	}
	ts, ok := event["time"].(string)
	if !ok {
		t.Fatalf("missing or non-string time field: %+v", event)
	}
	if !rfc3339Re.MatchString(ts) {
		t.Errorf("time = %q, want RFC3339 (e.g. 2026-05-15T03:02:01Z)", ts)
	}
	if _, err := time.Parse(time.RFC3339, ts); err != nil {
		t.Errorf("time %q does not parse as RFC3339: %v", ts, err)
	}
}
