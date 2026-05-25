// Package metrics is a tiny in-tree Prometheus text-format exporter.
//
// It implements just enough of the protocol — counters, gauges, and a
// simple histogram with fixed bucket bounds — to expose process-level
// telemetry without pulling in the upstream client library. Add new
// metric types here only when a planned dashboard actually needs them.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// microsecondsPerSecond converts seconds to integer microseconds for
// the histogram sum, which is stored as a uint64 to keep Observe lock-free.
const microsecondsPerSecond = 1e6

// LabelPair is the canonical key/value form for a single label.
type LabelPair struct {
	Name  string
	Value string
}

// labelsKey serialises labels in name=value form, sorted, so that two
// instances of the same metric+labels collapse to one series.
func labelsKey(pairs []LabelPair) string {
	if len(pairs) == 0 {
		return ""
	}
	sorted := make([]LabelPair, len(pairs))
	copy(sorted, pairs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var sb strings.Builder
	for i, lp := range sorted {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(lp.Name)
		sb.WriteByte('=')
		sb.WriteString(escapeLabelValue(lp.Value))
	}

	return sb.String()
}

// escapeLabelValue applies Prometheus quoting rules.
func escapeLabelValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)

	return `"` + v + `"`
}

// Counter is a monotonic uint64 counter.
type Counter struct {
	value atomic.Uint64
}

// Add increments by delta.
func (c *Counter) Add(delta uint64) { c.value.Add(delta) }

// Inc adds 1.
func (c *Counter) Inc() { c.value.Add(1) }

// Value returns the current value.
func (c *Counter) Value() uint64 { return c.value.Load() }

// Gauge is an int64 gauge — settable up or down.
type Gauge struct {
	value atomic.Int64
}

// Set replaces the gauge value.
func (g *Gauge) Set(v int64) { g.value.Store(v) }

// Add adjusts the gauge by delta (may be negative).
func (g *Gauge) Add(delta int64) { g.value.Add(delta) }

// Value returns the current value.
func (g *Gauge) Value() int64 { return g.value.Load() }

// Histogram counts samples into fixed buckets plus a running sum.
type Histogram struct {
	buckets []float64
	counts  []atomic.Uint64
	sum     atomic.Uint64 // microsecond-scale; treat as integer cents-of-microseconds
	total   atomic.Uint64
}

// NewHistogram constructs a Histogram with the given upper bucket
// bounds (in seconds, ascending). An implicit +Inf bucket is appended.
func NewHistogram(buckets []float64) *Histogram {
	sorted := make([]float64, len(buckets))
	copy(sorted, buckets)
	sort.Float64s(sorted)

	return &Histogram{
		buckets: sorted,
		counts:  make([]atomic.Uint64, len(sorted)+1),
	}
}

// Observe records a sample in seconds.
func (h *Histogram) Observe(seconds float64) {
	idx := sort.SearchFloat64s(h.buckets, seconds)
	if idx >= len(h.counts) {
		idx = len(h.counts) - 1
	}
	h.counts[idx].Add(1)
	h.total.Add(1)
	// Sum is microseconds — uint64 is fine for huge totals.
	h.sum.Add(uint64(seconds * microsecondsPerSecond))
}

// Total returns the count of observations.
func (h *Histogram) Total() uint64 { return h.total.Load() }

// SumSeconds returns the running sum of all observations in seconds.
func (h *Histogram) SumSeconds() float64 {
	return float64(h.sum.Load()) / microsecondsPerSecond
}

// snapshot returns cumulative bucket counts.
func (h *Histogram) snapshot() []uint64 {
	out := make([]uint64, len(h.counts))
	var sum uint64
	for i := range h.counts {
		sum += h.counts[i].Load()
		out[i] = sum
	}

	return out
}

type metricKind int

const (
	kindCounter metricKind = iota
	kindGauge
	kindHistogram
)

type metricSpec struct {
	name   string
	help   string
	kind   metricKind
	labels []string
}

type counterSeries struct {
	c      *Counter
	labels []LabelPair
}

type gaugeSeries struct {
	g      *Gauge
	labels []LabelPair
}

type histogramSeries struct {
	h      *Histogram
	labels []LabelPair
}

// Registry holds named metric families. Series within a family are
// keyed by their labels.
type Registry struct {
	mu     sync.Mutex
	specs  []metricSpec
	specBy map[string]*metricSpec

	counters   map[string]map[string]*counterSeries   // name -> labelsKey -> series
	gauges     map[string]map[string]*gaugeSeries     //
	histograms map[string]map[string]*histogramSeries //
}

// New constructs an empty Registry.
func New() *Registry {
	return &Registry{
		specBy:     map[string]*metricSpec{},
		counters:   map[string]map[string]*counterSeries{},
		gauges:     map[string]map[string]*gaugeSeries{},
		histograms: map[string]map[string]*histogramSeries{},
	}
}

// Counter returns a Counter for the named metric + label values. Help
// is used the first time the metric is registered.
func (r *Registry) Counter(name, help string, labels ...LabelPair) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registerSpec(name, help, kindCounter, labels)
	family := r.counters[name]
	if family == nil {
		family = map[string]*counterSeries{}
		r.counters[name] = family
	}
	key := labelsKey(labels)
	s := family[key]
	if s == nil {
		s = &counterSeries{c: &Counter{}, labels: append([]LabelPair(nil), labels...)}
		family[key] = s
	}

	return s.c
}

// Gauge returns a Gauge for the named metric + label values.
func (r *Registry) Gauge(name, help string, labels ...LabelPair) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registerSpec(name, help, kindGauge, labels)
	family := r.gauges[name]
	if family == nil {
		family = map[string]*gaugeSeries{}
		r.gauges[name] = family
	}
	key := labelsKey(labels)
	s := family[key]
	if s == nil {
		s = &gaugeSeries{g: &Gauge{}, labels: append([]LabelPair(nil), labels...)}
		family[key] = s
	}

	return s.g
}

// Histogram returns or creates a Histogram with the given buckets.
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...LabelPair) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registerSpec(name, help, kindHistogram, labels)
	family := r.histograms[name]
	if family == nil {
		family = map[string]*histogramSeries{}
		r.histograms[name] = family
	}
	key := labelsKey(labels)
	s := family[key]
	if s == nil {
		s = &histogramSeries{h: NewHistogram(buckets), labels: append([]LabelPair(nil), labels...)}
		family[key] = s
	}

	return s.h
}

// Label is a convenience constructor for one label.
func Label(name, value string) LabelPair { return LabelPair{Name: name, Value: value} }

func (r *Registry) registerSpec(name, help string, kind metricKind, labels []LabelPair) {
	if existing, ok := r.specBy[name]; ok {
		// Keep first registration; ignore subsequent help/kind differences.
		_ = existing

		return
	}
	labelNames := make([]string, 0, len(labels))
	for _, lp := range labels {
		labelNames = append(labelNames, lp.Name)
	}
	spec := metricSpec{name: name, help: help, kind: kind, labels: labelNames}
	r.specs = append(r.specs, spec)
	r.specBy[name] = &r.specs[len(r.specs)-1]
}

// WriteTo emits the registry contents in Prometheus text format.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var written int64
	specs := make([]metricSpec, len(r.specs))
	copy(specs, r.specs)
	sort.Slice(specs, func(i, j int) bool { return specs[i].name < specs[j].name })

	for _, spec := range specs {
		n, err := r.writeFamily(w, spec)
		written += n
		if err != nil {
			return written, err
		}
	}

	return written, nil
}

func (r *Registry) writeFamily(w io.Writer, spec metricSpec) (int64, error) {
	var written int64
	helpLine := fmt.Sprintf("# HELP %s %s\n", spec.name, escapeHelp(spec.help))
	typeLine := fmt.Sprintf("# TYPE %s %s\n", spec.name, kindTypeName(spec.kind))
	n, err := io.WriteString(w, helpLine+typeLine)
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("write header: %w", err)
	}

	switch spec.kind {
	case kindCounter:
		for _, s := range sortedCounters(r.counters[spec.name]) {
			n, err := fmt.Fprintf(w, "%s%s %d\n", spec.name, renderLabels(s.labels), s.c.Value())
			written += int64(n)
			if err != nil {
				return written, fmt.Errorf("write counter: %w", err)
			}
		}
	case kindGauge:
		for _, s := range sortedGauges(r.gauges[spec.name]) {
			n, err := fmt.Fprintf(w, "%s%s %d\n", spec.name, renderLabels(s.labels), s.g.Value())
			written += int64(n)
			if err != nil {
				return written, fmt.Errorf("write gauge: %w", err)
			}
		}
	case kindHistogram:
		for _, s := range sortedHistograms(r.histograms[spec.name]) {
			n, err := writeHistogram(w, spec.name, s)
			written += n
			if err != nil {
				return written, err
			}
		}
	}

	return written, nil
}

func writeHistogram(w io.Writer, name string, s *histogramSeries) (int64, error) {
	var written int64
	cumulative := s.h.snapshot()
	for i, b := range s.h.buckets {
		labels := append([]LabelPair{}, s.labels...)
		labels = append(labels, LabelPair{Name: "le", Value: strconv.FormatFloat(b, 'g', -1, 64)})
		n, err := fmt.Fprintf(w, "%s_bucket%s %d\n", name, renderLabels(labels), cumulative[i])
		written += int64(n)
		if err != nil {
			return written, fmt.Errorf("write histogram bucket: %w", err)
		}
	}
	infLabels := append([]LabelPair{}, s.labels...)
	infLabels = append(infLabels, LabelPair{Name: "le", Value: "+Inf"})
	n, err := fmt.Fprintf(w, "%s_bucket%s %d\n", name, renderLabels(infLabels), cumulative[len(cumulative)-1])
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("write histogram +Inf: %w", err)
	}
	n, err = fmt.Fprintf(w, "%s_sum%s %g\n", name, renderLabels(s.labels), s.h.SumSeconds())
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("write histogram sum: %w", err)
	}
	n, err = fmt.Fprintf(w, "%s_count%s %d\n", name, renderLabels(s.labels), s.h.Total())
	written += int64(n)
	if err != nil {
		return written, fmt.Errorf("write histogram count: %w", err)
	}

	return written, nil
}

func renderLabels(labels []LabelPair) string {
	if len(labels) == 0 {
		return ""
	}
	sorted := make([]LabelPair, len(labels))
	copy(sorted, labels)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var sb strings.Builder
	sb.WriteByte('{')
	for i, lp := range sorted {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(lp.Name)
		sb.WriteByte('=')
		sb.WriteString(escapeLabelValue(lp.Value))
	}
	sb.WriteByte('}')

	return sb.String()
}

func sortedCounters(m map[string]*counterSeries) []*counterSeries {
	out := make([]*counterSeries, 0, len(m))
	for _, s := range m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return labelsKey(out[i].labels) < labelsKey(out[j].labels) })

	return out
}

func sortedGauges(m map[string]*gaugeSeries) []*gaugeSeries {
	out := make([]*gaugeSeries, 0, len(m))
	for _, s := range m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return labelsKey(out[i].labels) < labelsKey(out[j].labels) })

	return out
}

func sortedHistograms(m map[string]*histogramSeries) []*histogramSeries {
	out := make([]*histogramSeries, 0, len(m))
	for _, s := range m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return labelsKey(out[i].labels) < labelsKey(out[j].labels) })

	return out
}

func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "\n", `\n`)

	return s
}

func kindTypeName(k metricKind) string {
	switch k {
	case kindCounter:
		return "counter"
	case kindGauge:
		return "gauge"
	case kindHistogram:
		return "histogram"
	}

	return "untyped"
}
