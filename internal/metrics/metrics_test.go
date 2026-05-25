package metrics_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/eunmann/s3-inv-db/internal/metrics"
)

func TestCounterAndGaugeEmit(t *testing.T) {
	r := metrics.New()
	c := r.Counter("s3invdb_requests_total", "HTTP requests", metrics.Label("kind", "stats"))
	c.Add(7)
	g := r.Gauge("s3invdb_loaded_inventories", "Loaded inventories")
	g.Set(3)

	var buf bytes.Buffer
	if _, err := r.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "s3invdb_requests_total{kind=\"stats\"} 7") {
		t.Errorf("counter output missing: %q", out)
	}
	if !strings.Contains(out, "s3invdb_loaded_inventories 3") {
		t.Errorf("gauge output missing: %q", out)
	}
	if !strings.Contains(out, "# TYPE s3invdb_requests_total counter") {
		t.Errorf("missing TYPE line: %q", out)
	}
}

func TestHistogramBuckets(t *testing.T) {
	r := metrics.New()
	h := r.Histogram("s3invdb_query_seconds", "query latency", []float64{0.001, 0.01, 0.1})
	h.Observe(0.0005)
	h.Observe(0.05)
	h.Observe(0.5)

	var buf bytes.Buffer
	if _, err := r.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	out := buf.String()
	mustContain := []string{
		"s3invdb_query_seconds_bucket{le=\"0.001\"} 1",
		"s3invdb_query_seconds_bucket{le=\"0.01\"} 1",
		"s3invdb_query_seconds_bucket{le=\"0.1\"} 2",
		"s3invdb_query_seconds_bucket{le=\"+Inf\"} 3",
		"s3invdb_query_seconds_count 3",
	}
	for _, want := range mustContain {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output:\n%s", want, out)
		}
	}
}

func TestDuplicateRegistrationReturnsSameInstance(t *testing.T) {
	r := metrics.New()
	c1 := r.Counter("metric_a", "first")
	c2 := r.Counter("metric_a", "second")
	c1.Inc()
	if c1.Value() != c2.Value() {
		t.Errorf("same-name counter not shared: %d vs %d", c1.Value(), c2.Value())
	}
}
