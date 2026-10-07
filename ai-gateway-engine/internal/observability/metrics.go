package observability

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter tracks monotonically increasing counts.
type Counter struct {
	mu     sync.RWMutex
	values map[string]*atomic.Uint64
}

func newCounter() *Counter {
	return &Counter{
		values: make(map[string]*atomic.Uint64),
	}
}

func (c *Counter) Inc(labels ...string) {
	c.Add(1, labels...)
}

func (c *Counter) Add(delta uint64, labels ...string) {
	key := strings.Join(labels, ",")
	c.mu.RLock()
	val, exists := c.values[key]
	c.mu.RUnlock()

	if exists {
		val.Add(delta)
		return
	}

	c.mu.Lock()
	val, exists = c.values[key]
	if !exists {
		val = &atomic.Uint64{}
		c.values[key] = val
	}
	c.mu.Unlock()

	val.Add(delta)
}

func (c *Counter) Get(labels ...string) uint64 {
	key := strings.Join(labels, ",")
	c.mu.RLock()
	defer c.mu.RUnlock()
	if val, ok := c.values[key]; ok {
		return val.Load()
	}
	return 0
}

// Histogram tracks sample distributions across predefined buckets.
type Histogram struct {
	mu      sync.RWMutex
	buckets []float64 // Sorted boundaries in seconds
	counts  map[string][]uint64
	sums    map[string]float64
}

func newHistogram(buckets []float64) *Histogram {
	sort.Float64s(buckets)
	return &Histogram{
		buckets: buckets,
		counts:  make(map[string][]uint64),
		sums:    make(map[string]float64),
	}
}

// Observe records a duration observation in seconds.
func (h *Histogram) Observe(val float64, labels ...string) {
	key := strings.Join(labels, ",")
	h.mu.Lock()
	defer h.mu.Unlock()

	bCounts, exists := h.counts[key]
	if !exists {
		bCounts = make([]uint64, len(h.buckets)+1)
		h.counts[key] = bCounts
	}

	h.sums[key] += val

	for i, b := range h.buckets {
		if val <= b {
			bCounts[i]++
		}
	}
	// +Inf bucket counts all observations
	bCounts[len(h.buckets)]++
}

// Registry stores all application metrics for Prometheus scraping.
type Registry struct {
	// Custom LLM Metrics
	LLMTimeToFirstTokenSeconds *Histogram
	LLMTokensGeneratedTotal    *Counter
	SemanticCacheHitsTotal     *Counter
	SemanticCacheMissesTotal   *Counter
	FallbackTriggeredTotal     *Counter

	// HTTP & Gateway Metrics
	HTTPRequestsTotal        *Counter
	HTTPRequestDurationSeconds *Histogram
}

var (
	defaultRegistry *Registry
	onceRegistry    sync.Once
)

// DefaultBuckets for latency histograms in seconds (10ms to 10s).
var DefaultBuckets = []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0}

// DefaultRegistry returns the global metrics registry singleton.
func DefaultRegistry() *Registry {
	onceRegistry.Do(func() {
		defaultRegistry = NewRegistry()
	})
	return defaultRegistry
}

// NewRegistry initializes a new metrics registry with all required metrics.
func NewRegistry() *Registry {
	return &Registry{
		LLMTimeToFirstTokenSeconds: newHistogram(DefaultBuckets),
		LLMTokensGeneratedTotal:    newCounter(),
		SemanticCacheHitsTotal:     newCounter(),
		SemanticCacheMissesTotal:   newCounter(),
		FallbackTriggeredTotal:     newCounter(),
		HTTPRequestsTotal:          newCounter(),
		HTTPRequestDurationSeconds: newHistogram(DefaultBuckets),
	}
}

// WritePrometheusText exports metrics in standard Prometheus exposition format.
func (r *Registry) WritePrometheusText(w io.Writer) {
	// 1. LLM Time to First Token (TTFT)
	fmt.Fprintln(w, "# HELP llm_time_to_first_token_seconds Time from request dispatch to first token received from provider.")
	fmt.Fprintln(w, "# TYPE llm_time_to_first_token_seconds histogram")
	r.writeHistogram(w, "llm_time_to_first_token_seconds", r.LLMTimeToFirstTokenSeconds, []string{"provider", "model", "cached"})

	// 2. LLM Tokens Generated Total
	fmt.Fprintln(w, "# HELP llm_tokens_generated_total Total output tokens emitted by LLM stream.")
	fmt.Fprintln(w, "# TYPE llm_tokens_generated_total counter")
	r.writeCounter(w, "llm_tokens_generated_total", r.LLMTokensGeneratedTotal, []string{"provider", "model"})

	// 3. Semantic Cache Hits / Misses
	fmt.Fprintln(w, "# HELP semantic_cache_hits_total Total number of semantic cache hits.")
	fmt.Fprintln(w, "# TYPE semantic_cache_hits_total counter")
	fmt.Fprintf(w, "semantic_cache_hits_total %d\n", r.SemanticCacheHitsTotal.Get())

	fmt.Fprintln(w, "# HELP semantic_cache_misses_total Total number of semantic cache misses.")
	fmt.Fprintln(w, "# TYPE semantic_cache_misses_total counter")
	fmt.Fprintf(w, "semantic_cache_misses_total %d\n", r.SemanticCacheMissesTotal.Get())

	// 4. Fallback Triggered Total
	fmt.Fprintln(w, "# HELP fallback_triggered_total Count of fallback events routed across providers.")
	fmt.Fprintln(w, "# TYPE fallback_triggered_total counter")
	r.writeCounter(w, "fallback_triggered_total", r.FallbackTriggeredTotal, []string{"from_provider", "to_provider", "reason"})

	// 5. HTTP Metrics
	fmt.Fprintln(w, "# HELP http_requests_total Total number of incoming HTTP requests.")
	fmt.Fprintln(w, "# TYPE http_requests_total counter")
	r.writeCounter(w, "http_requests_total", r.HTTPRequestsTotal, []string{"method", "path", "status"})

	fmt.Fprintln(w, "# HELP http_request_duration_seconds HTTP request latencies.")
	fmt.Fprintln(w, "# TYPE http_request_duration_seconds histogram")
	r.writeHistogram(w, "http_request_duration_seconds", r.HTTPRequestDurationSeconds, []string{"method", "path"})
}

func (r *Registry) writeCounter(w io.Writer, name string, c *Counter, labelNames []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for key, val := range c.values {
		parts := strings.Split(key, ",")
		var labelPairs []string
		for i, p := range parts {
			if i < len(labelNames) {
				labelPairs = append(labelPairs, fmt.Sprintf("%s=%q", labelNames[i], p))
			}
		}

		if len(labelPairs) > 0 {
			fmt.Fprintf(w, "%s{%s} %d\n", name, strings.Join(labelPairs, ","), val.Load())
		} else {
			fmt.Fprintf(w, "%s %d\n", name, val.Load())
		}
	}
}

func (r *Registry) writeHistogram(w io.Writer, name string, h *Histogram, labelNames []string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for key, bCounts := range h.counts {
		parts := strings.Split(key, ",")
		var baseLabels []string
		for i, p := range parts {
			if i < len(labelNames) {
				baseLabels = append(baseLabels, fmt.Sprintf("%s=%q", labelNames[i], p))
			}
		}
		baseLabelStr := strings.Join(baseLabels, ",")
		if baseLabelStr != "" {
			baseLabelStr += ","
		}

		for i, bound := range h.buckets {
			fmt.Fprintf(w, "%s_bucket{%sle=%q} %d\n", name, baseLabelStr, fmt.Sprintf("%g", bound), bCounts[i])
		}
		fmt.Fprintf(w, "%s_bucket{%sle=\"+Inf\"} %d\n", name, baseLabelStr, bCounts[len(h.buckets)])
		fmt.Fprintf(w, "%s_sum{%s} %g\n", name, strings.TrimRight(baseLabelStr, ","), h.sums[key])
		fmt.Fprintf(w, "%s_count{%s} %d\n", name, strings.TrimRight(baseLabelStr, ","), bCounts[len(h.buckets)])
	}
}

// Handler returns an http.Handler that serves metrics at /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.WritePrometheusText(w)
	})
}
