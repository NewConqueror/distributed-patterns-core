package sse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mockFlusherResponseWriter implements http.ResponseWriter and http.Flusher
type mockFlusherResponseWriter struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (m *mockFlusherResponseWriter) Flush() {
	m.flushed = true
}

func TestNewWriter_HeadersAndFlusher(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	rec := &mockFlusherResponseWriter{
		ResponseRecorder: httptest.NewRecorder(),
	}

	writer, err := NewWriter(rec, req)
	if err != nil {
		t.Fatalf("unexpected error creating SSE writer: %v", err)
	}
	if writer == nil {
		t.Fatal("expected non-nil writer")
	}

	headers := rec.Header()
	if headers.Get("Content-Type") != "text/event-stream" {
		t.Errorf("expected Content-Type text/event-stream, got %s", headers.Get("Content-Type"))
	}
	if headers.Get("Cache-Control") != "no-cache" {
		t.Errorf("expected Cache-Control no-cache, got %s", headers.Get("Cache-Control"))
	}
	if headers.Get("Connection") != "keep-alive" {
		t.Errorf("expected Connection keep-alive, got %s", headers.Get("Connection"))
	}
	if headers.Get("X-Accel-Buffering") != "no" {
		t.Errorf("expected X-Accel-Buffering no, got %s", headers.Get("X-Accel-Buffering"))
	}
	if !rec.flushed {
		t.Errorf("expected initial headers to be flushed")
	}
}

func TestNewWriter_UnsupportedFlusher(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	// standard httptest.ResponseRecorder does implement Flusher, so create non-flusher
	type nonFlusherWriter struct {
		http.ResponseWriter
	}
	w := nonFlusherWriter{httptest.NewRecorder()}

	_, err := NewWriter(w, req)
	if err != ErrStreamingNotSupported {
		t.Fatalf("expected ErrStreamingNotSupported, got %v", err)
	}
}

func TestWriter_WriteEventAndChunk(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	rec := &mockFlusherResponseWriter{
		ResponseRecorder: httptest.NewRecorder(),
	}

	writer, err := NewWriter(rec, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 1. Write structured event
	err = writer.WriteEvent(Event{
		ID:    "1",
		Event: "delta",
		Data:  map[string]string{"text": "hello"},
	})
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}

	// 2. Write chunk
	err = writer.WriteChunk("world")
	if err != nil {
		t.Fatalf("unexpected write chunk error: %v", err)
	}

	// 3. Write comment
	err = writer.WriteComment("ping")
	if err != nil {
		t.Fatalf("unexpected write comment error: %v", err)
	}

	// 4. Write done
	err = writer.WriteDone()
	if err != nil {
		t.Fatalf("unexpected write done error: %v", err)
	}

	body := rec.Body.String()

	expectedSubstrings := []string{
		"id: 1\nevent: delta\ndata: {\"text\":\"hello\"}\n\n",
		"data: world\n\n",
		": ping\n\n",
		"data: [DONE]\n\n",
	}

	for _, sub := range expectedSubstrings {
		if !strings.Contains(body, sub) {
			t.Errorf("expected body to contain %q, but got:\n%s", sub, body)
		}
	}
}

func TestWriter_ClientDisconnected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/stream", nil).WithContext(ctx)
	rec := &mockFlusherResponseWriter{
		ResponseRecorder: httptest.NewRecorder(),
	}

	writer, err := NewWriter(rec, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Cancel context to simulate client tab closing or aborting
	cancel()

	err = writer.WriteChunk("should not reach")
	if err != ErrClientDisconnected {
		t.Fatalf("expected ErrClientDisconnected, got %v", err)
	}

	err = writer.WriteDone()
	if err != ErrClientDisconnected {
		t.Fatalf("expected ErrClientDisconnected, got %v", err)
	}
}
