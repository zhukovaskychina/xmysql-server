package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Type describes the supported metric families.
type Type string

const (
	Counter   Type = "counter"
	Gauge     Type = "gauge"
	Histogram Type = "histogram"
)

// Labels are metric dimensions.
type Labels map[string]string

// Registry stores process-local metrics and can render them using the
// Prometheus text exposition format.
type Registry struct {
	mu       sync.RWMutex
	metrics  map[string]*metric
	exported time.Time
}

type metric struct {
	Name        string
	Help        string
	Type        Type
	LabelNames  []string
	Samples     map[string]*sample
	Buckets     []float64
	BucketCount map[string]map[float64]uint64
}

type sample struct {
	Labels Labels
	Value  float64
	Count  uint64
	Sum    float64
}

// NewRegistry creates an empty metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		metrics: make(map[string]*metric),
	}
}

// RegisterCounter registers a counter metric.
func (r *Registry) RegisterCounter(name, help string, labelNames ...string) {
	r.register(name, help, Counter, nil, labelNames...)
}

// RegisterGauge registers a gauge metric.
func (r *Registry) RegisterGauge(name, help string, labelNames ...string) {
	r.register(name, help, Gauge, nil, labelNames...)
}

// RegisterHistogram registers a histogram metric.
func (r *Registry) RegisterHistogram(name, help string, buckets []float64, labelNames ...string) {
	if len(buckets) == 0 {
		buckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}
	}
	sorted := append([]float64(nil), buckets...)
	sort.Float64s(sorted)
	r.register(name, help, Histogram, sorted, labelNames...)
}

func (r *Registry) register(name, help string, metricType Type, buckets []float64, labelNames ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.metrics[name] = &metric{
		Name:        name,
		Help:        help,
		Type:        metricType,
		LabelNames:  append([]string(nil), labelNames...),
		Samples:     make(map[string]*sample),
		Buckets:     buckets,
		BucketCount: make(map[string]map[float64]uint64),
	}
}

// IncCounter increments a counter by delta.
func (r *Registry) IncCounter(name string, delta float64, labels Labels) error {
	if delta < 0 {
		return fmt.Errorf("counter delta must be non-negative")
	}
	return r.updateValue(name, Counter, delta, labels, true)
}

// SetGauge sets a gauge value.
func (r *Registry) SetGauge(name string, value float64, labels Labels) error {
	return r.updateValue(name, Gauge, value, labels, false)
}

// AddGauge adds delta to a gauge value.
func (r *Registry) AddGauge(name string, delta float64, labels Labels) error {
	return r.updateValue(name, Gauge, delta, labels, true)
}

func (r *Registry) updateValue(name string, expected Type, value float64, labels Labels, add bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	m, ok := r.metrics[name]
	if !ok {
		return fmt.Errorf("metric %s is not registered", name)
	}
	if m.Type != expected {
		return fmt.Errorf("metric %s is %s, not %s", name, m.Type, expected)
	}

	key := labelsKey(labels, m.LabelNames)
	s := m.Samples[key]
	if s == nil {
		s = &sample{Labels: copyLabels(labels)}
		m.Samples[key] = s
	}

	if add {
		s.Value += value
	} else {
		s.Value = value
	}
	r.exported = time.Now()
	return nil
}

// ObserveHistogram records an observation.
func (r *Registry) ObserveHistogram(name string, value float64, labels Labels) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	m, ok := r.metrics[name]
	if !ok {
		return fmt.Errorf("metric %s is not registered", name)
	}
	if m.Type != Histogram {
		return fmt.Errorf("metric %s is %s, not histogram", name, m.Type)
	}

	key := labelsKey(labels, m.LabelNames)
	s := m.Samples[key]
	if s == nil {
		s = &sample{Labels: copyLabels(labels)}
		m.Samples[key] = s
	}
	s.Count++
	s.Sum += value

	counts := m.BucketCount[key]
	if counts == nil {
		counts = make(map[float64]uint64)
		m.BucketCount[key] = counts
	}
	for _, bucket := range m.Buckets {
		if value <= bucket {
			counts[bucket]++
		}
	}
	r.exported = time.Now()
	return nil
}

// WritePrometheusText renders the registry using Prometheus text exposition.
func (r *Registry) WritePrometheusText() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.metrics))
	for name := range r.metrics {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	for _, name := range names {
		m := r.metrics[name]
		b.WriteString("# HELP ")
		b.WriteString(m.Name)
		b.WriteByte(' ')
		b.WriteString(escapeHelp(m.Help))
		b.WriteByte('\n')
		b.WriteString("# TYPE ")
		b.WriteString(m.Name)
		b.WriteByte(' ')
		b.WriteString(string(m.Type))
		b.WriteByte('\n')

		switch m.Type {
		case Counter, Gauge:
			writeValueSamples(&b, m)
		case Histogram:
			writeHistogramSamples(&b, m)
		}
	}

	return b.String()
}

func writeValueSamples(b *strings.Builder, m *metric) {
	keys := sortedSampleKeys(m.Samples)
	for _, key := range keys {
		s := m.Samples[key]
		b.WriteString(m.Name)
		b.WriteString(formatLabels(s.Labels, m.LabelNames))
		b.WriteByte(' ')
		b.WriteString(formatFloat(s.Value))
		b.WriteByte('\n')
	}
}

func writeHistogramSamples(b *strings.Builder, m *metric) {
	keys := sortedSampleKeys(m.Samples)
	for _, key := range keys {
		s := m.Samples[key]
		counts := m.BucketCount[key]
		for _, bucket := range m.Buckets {
			labels := copyLabels(s.Labels)
			labels["le"] = formatFloat(bucket)
			labelNames := append([]string(nil), m.LabelNames...)
			labelNames = append(labelNames, "le")
			b.WriteString(m.Name)
			b.WriteString("_bucket")
			b.WriteString(formatLabels(labels, labelNames))
			b.WriteByte(' ')
			b.WriteString(fmt.Sprintf("%d", counts[bucket]))
			b.WriteByte('\n')
		}

		labels := copyLabels(s.Labels)
		labels["le"] = "+Inf"
		labelNames := append([]string(nil), m.LabelNames...)
		labelNames = append(labelNames, "le")
		b.WriteString(m.Name)
		b.WriteString("_bucket")
		b.WriteString(formatLabels(labels, labelNames))
		b.WriteByte(' ')
		b.WriteString(fmt.Sprintf("%d", s.Count))
		b.WriteByte('\n')

		b.WriteString(m.Name)
		b.WriteString("_sum")
		b.WriteString(formatLabels(s.Labels, m.LabelNames))
		b.WriteByte(' ')
		b.WriteString(formatFloat(s.Sum))
		b.WriteByte('\n')

		b.WriteString(m.Name)
		b.WriteString("_count")
		b.WriteString(formatLabels(s.Labels, m.LabelNames))
		b.WriteByte(' ')
		b.WriteString(fmt.Sprintf("%d", s.Count))
		b.WriteByte('\n')
	}
}

func sortedSampleKeys(samples map[string]*sample) []string {
	keys := make([]string, 0, len(samples))
	for key := range samples {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func labelsKey(labels Labels, labelNames []string) string {
	if len(labelNames) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labelNames))
	for _, name := range labelNames {
		parts = append(parts, name+"="+labels[name])
	}
	return strings.Join(parts, "\xff")
}

func copyLabels(labels Labels) Labels {
	copied := make(Labels, len(labels))
	for key, value := range labels {
		copied[key] = value
	}
	return copied
}

func formatLabels(labels Labels, labelNames []string) string {
	if len(labelNames) == 0 {
		return ""
	}
	parts := make([]string, 0, len(labelNames))
	for _, name := range labelNames {
		parts = append(parts, name+"=\""+escapeLabel(labels[name])+"\"")
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escapeHelp(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	return value
}

func escapeLabel(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return value
}

func formatFloat(value float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.6f", value), "0"), ".")
}
