package sse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
)

var (
	// ErrStreamingNotSupported is returned when the ResponseWriter does not implement http.Flusher.
	ErrStreamingNotSupported = errors.New("streaming unsupported: ResponseWriter does not implement http.Flusher")
	// ErrClientDisconnected is returned when the client connection has been canceled.
	ErrClientDisconnected = errors.New("client disconnected")
)

// Writer provides a clean abstraction for Server-Sent Events (SSE).
type Writer struct {
	w       http.ResponseWriter
	flusher http.Flusher
	ctx     context.Context
	mu      sync.Mutex
}

// Event defines the payload for an SSE message.
type Event struct {
	ID    string
	Event string
	Data  any
}

// NewWriter initializes and sets the necessary HTTP headers for SSE.
// It verifies that http.ResponseWriter implements http.Flusher.
func NewWriter(w http.ResponseWriter, r *http.Request) (*Writer, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, ErrStreamingNotSupported
	}

	// Set required SSE headers
	headers := w.Header()
	headers.Set("Content-Type", "text/event-stream")
	headers.Set("Cache-Control", "no-cache")
	headers.Set("Connection", "keep-alive")
	// Prevent nginx/reverse proxy buffering
	headers.Set("X-Accel-Buffering", "no")

	// Flush initial headers to establish the SSE stream with client immediately
	flusher.Flush()

	return &Writer{
		w:       w,
		flusher: flusher,
		ctx:     r.Context(),
	}, nil
}

// WriteEvent writes a formatted SSE event (id, event name, data) and immediately flushes the buffer.
func (sw *Writer) WriteEvent(evt Event) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// Check if client disconnected before attempting to write
	select {
	case <-sw.ctx.Done():
		return ErrClientDisconnected
	default:
	}

	var dataBytes []byte
	switch v := evt.Data.(type) {
	case []byte:
		dataBytes = v
	case string:
		dataBytes = []byte(v)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed to marshal sse data: %w", err)
		}
		dataBytes = b
	}

	if evt.ID != "" {
		if _, err := fmt.Fprintf(sw.w, "id: %s\n", evt.ID); err != nil {
			return err
		}
	}

	if evt.Event != "" {
		if _, err := fmt.Fprintf(sw.w, "event: %s\n", evt.Event); err != nil {
			return err
		}
	}

	if _, err := fmt.Fprintf(sw.w, "data: %s\n\n", dataBytes); err != nil {
		return err
	}

	sw.flusher.Flush()
	return nil
}

// WriteChunk writes a data-only SSE message (standard OpenAI format: "data: <json>\n\n").
func (sw *Writer) WriteChunk(data any) error {
	return sw.WriteEvent(Event{
		Data: data,
	})
}

// WriteComment sends an SSE comment (e.g., ": keep-alive\n\n") to keep the connection warm.
func (sw *Writer) WriteComment(comment string) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	select {
	case <-sw.ctx.Done():
		return ErrClientDisconnected
	default:
	}

	if _, err := fmt.Fprintf(sw.w, ": %s\n\n", comment); err != nil {
		return err
	}

	sw.flusher.Flush()
	return nil
}

// WriteDone sends the standard SSE stream terminator "data: [DONE]\n\n".
func (sw *Writer) WriteDone() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	select {
	case <-sw.ctx.Done():
		return ErrClientDisconnected
	default:
	}

	if _, err := fmt.Fprint(sw.w, "data: [DONE]\n\n"); err != nil {
		return err
	}

	sw.flusher.Flush()
	return nil
}
