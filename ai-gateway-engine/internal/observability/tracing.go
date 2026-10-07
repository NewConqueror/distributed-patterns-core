package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// SpanStatusCode represents OpenTelemetry status code.
type SpanStatusCode int

const (
	StatusUnset SpanStatusCode = 0
	StatusOK    SpanStatusCode = 1
	StatusError  SpanStatusCode = 2
)

// SpanEvent represents a timestamped event inside a Span (e.g. TTFT recorded).
type SpanEvent struct {
	Name       string         `json:"name"`
	Timestamp  time.Time      `json:"timestamp"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// Span represents a single unit of work in distributed tracing conforming to OpenTelemetry.
type Span struct {
	TraceID      string         `json:"trace_id"`
	SpanID       string         `json:"span_id"`
	ParentSpanID string         `json:"parent_span_id,omitempty"`
	Name         string         `json:"name"`
	StartTime    time.Time      `json:"start_time"`
	EndTime      time.Time      `json:"end_time"`
	DurationMS   float64        `json:"duration_ms"`
	Attributes   map[string]any `json:"attributes"`
	Events       []SpanEvent    `json:"events,omitempty"`
	Status       SpanStatusCode `json:"status"`
	StatusDesc   string         `json:"status_description,omitempty"`

	mu     sync.Mutex
	tracer *Tracer
	ended  bool
}

// SetAttribute sets a key-value attribute on the span.
func (s *Span) SetAttribute(key string, val any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Attributes[key] = val
}

// AddEvent records a point-in-time event on the span.
func (s *Span) AddEvent(name string, attrs map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Events = append(s.Events, SpanEvent{
		Name:       name,
		Timestamp:  time.Now(),
		Attributes: attrs,
	})
}

// SetStatus marks span status as OK or Error.
func (s *Span) SetStatus(code SpanStatusCode, desc string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Status = code
	s.StatusDesc = desc
}

// End marks the completion of the span.
func (s *Span) End() {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.EndTime = time.Now()
	s.DurationMS = float64(s.EndTime.Sub(s.StartTime).Microseconds()) / 1000.0
	s.mu.Unlock()

	if s.tracer != nil {
		s.tracer.recordCompletedSpan(s)
	}
}

type spanContextKey struct{}

var activeSpanKey = spanContextKey{}

// Tracer creates and manages Spans.
type Tracer struct {
	mu             sync.RWMutex
	completedSpans []*Span
	maxStoredSpans int
}

var (
	defaultTracer *Tracer
	onceTracer    sync.Once
)

// DefaultTracer returns the global Tracer singleton.
func DefaultTracer() *Tracer {
	onceTracer.Do(func() {
		defaultTracer = NewTracer(1000)
	})
	return defaultTracer
}

// NewTracer creates an OpenTelemetry-compatible Tracer.
func NewTracer(maxStored int) *Tracer {
	if maxStored <= 0 {
		maxStored = 1000
	}
	return &Tracer{
		completedSpans: make([]*Span, 0, maxStored),
		maxStoredSpans: maxStored,
	}
}

// StartSpan creates a child span or root span and injects it into context.
func (t *Tracer) StartSpan(ctx context.Context, name string) (context.Context, *Span) {
	var traceID, parentSpanID string

	// 1. Check if parent span exists in context
	if parent, ok := ctx.Value(activeSpanKey).(*Span); ok && parent != nil {
		traceID = parent.TraceID
		parentSpanID = parent.SpanID
	}

	// 2. If no parent, check incoming W3C traceparent header or generate new TraceID
	if traceID == "" {
		traceID = generateHex(16) // 128-bit ID (32 hex chars)
	}

	span := &Span{
		TraceID:      traceID,
		SpanID:       generateHex(8), // 64-bit ID (16 hex chars)
		ParentSpanID: parentSpanID,
		Name:         name,
		StartTime:    time.Now(),
		Attributes:   make(map[string]any),
		Events:       make([]SpanEvent, 0),
		Status:       StatusOK,
		tracer:       t,
	}

	return context.WithValue(ctx, activeSpanKey, span), span
}

// SpanFromContext extracts the current active Span from context.
func SpanFromContext(ctx context.Context) *Span {
	if span, ok := ctx.Value(activeSpanKey).(*Span); ok {
		return span
	}
	return nil
}

func (t *Tracer) recordCompletedSpan(s *Span) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.completedSpans) >= t.maxStoredSpans {
		// Evict oldest
		t.completedSpans = t.completedSpans[1:]
	}
	t.completedSpans = append(t.completedSpans, s)
}

// GetSpans returns recorded spans, optionally filtered by TraceID.
func (t *Tracer) GetSpans(traceID string) []*Span {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if traceID == "" {
		res := make([]*Span, len(t.completedSpans))
		copy(res, t.completedSpans)
		return res
	}

	var filtered []*Span
	for _, s := range t.completedSpans {
		if s.TraceID == traceID {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

// TracesHandler serves JSON traces for inspection or Jaeger/OTLP ingestion.
func (t *Tracer) TracesHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.URL.Query().Get("trace_id")
		spans := t.GetSpans(traceID)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"total_spans": len(spans),
			"spans":       spans,
		})
	})
}

// ExtractTraceparent reads standard W3C traceparent header: 00-{traceid}-{spanid}-{flags}.
func ExtractTraceparent(headerVal string) (traceID string, parentID string) {
	parts := strings.Split(headerVal, "-")
	if len(parts) >= 3 && parts[0] == "00" {
		return parts[1], parts[2]
	}
	return "", ""
}

// FormatTraceparent formats standard W3C traceparent header.
func FormatTraceparent(span *Span) string {
	if span == nil {
		return ""
	}
	return fmt.Sprintf("00-%s-%s-01", span.TraceID, span.SpanID)
}

func generateHex(bytesLen int) string {
	b := make([]byte, bytesLen)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
