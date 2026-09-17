package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ResponseSummary provides metadata regarding a completed upstream forward operation.
type ResponseSummary struct {
	StatusCode   int
	BytesWritten int64
	Duration     time.Duration
}

// Forwarder manages HTTP request and response forwarding between clients and upstream targets.
type Forwarder struct {
	transport  http.RoundTripper
	bufferPool *sync.Pool
}

// NewForwarder creates a new reverse proxy forwarder.
// If transport is nil, DefaultTransportConfig() will be used to construct a pooled transport.
func NewForwarder(transport http.RoundTripper) *Forwarder {
	if transport == nil {
		transport = NewPooledTransport(DefaultTransportConfig())
	}

	return &Forwarder{
		transport: transport,
		bufferPool: &sync.Pool{
			New: func() any {
				buf := make([]byte, 32*1024)
				return &buf
			},
		},
	}
}

// Forward sends an incoming HTTP request to the specified target upstream URL and streams the response back.
// It executes hop-by-hop header removal (RFC 7230), X-Forwarded-* header enrichment,
// status code propagation, and memory-efficient chunked response body streaming.
func (f *Forwarder) Forward(w http.ResponseWriter, req *http.Request, target *url.URL) (*ResponseSummary, error) {
	outboundReq := req.Clone(req.Context())

	// 1. Rewrite URL Scheme, Host, Path and Query
	outboundReq.URL.Scheme = target.Scheme
	outboundReq.URL.Host = target.Host
	outboundReq.URL.Path = singleJoiningSlash(target.Path, req.URL.Path)
	outboundReq.URL.RawQuery = req.URL.RawQuery
	outboundReq.Host = target.Host

	// 2. RFC 7230 hop-by-hop header stripping on request
	RemoveHopByHopHeaders(outboundReq.Header)

	// 3. Inject and update proxy forwarding headers (X-Forwarded-For, X-Real-IP, etc.)
	SetForwardingHeaders(outboundReq, "")

	// 4. Dispatch round-trip to upstream
	start := time.Now()
	resp, err := f.transport.RoundTrip(outboundReq)
	duration := time.Since(start)

	if err != nil {
		return f.handleUpstreamError(w, outboundReq.Context(), err, duration)
	}
	defer resp.Body.Close()

	// 5. RFC 7230 hop-by-hop header stripping on response
	RemoveHopByHopHeaders(resp.Header)

	// 6. Copy headers from upstream response to downstream writer
	downstreamHeader := w.Header()
	for key, values := range resp.Header {
		for _, value := range values {
			downstreamHeader.Add(key, value)
		}
	}

	// 7. Write status code
	w.WriteHeader(resp.StatusCode)

	// 8. Stream response body using buffer pool
	bufPtr := f.bufferPool.Get().(*[]byte)
	defer f.bufferPool.Put(bufPtr)

	bytesWritten, copyErr := io.CopyBuffer(w, resp.Body, *bufPtr)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	summary := &ResponseSummary{
		StatusCode:   resp.StatusCode,
		BytesWritten: bytesWritten,
		Duration:     duration,
	}

	if copyErr != nil && !errors.Is(copyErr, context.Canceled) {
		return summary, copyErr
	}

	return summary, nil
}

// handleUpstreamError inspects network/context errors and writes appropriate HTTP error codes.
func (f *Forwarder) handleUpstreamError(w http.ResponseWriter, ctx context.Context, err error, duration time.Duration) (*ResponseSummary, error) {
	statusCode := http.StatusBadGateway

	if errors.Is(ctx.Err(), context.Canceled) {
		// Client disconnected before upstream finished
		statusCode = 499 // Nginx convention: Client Closed Request
		return &ResponseSummary{StatusCode: statusCode, Duration: duration}, err
	}

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		statusCode = http.StatusGatewayTimeout
		http.Error(w, "Gateway Timeout", statusCode)
		return &ResponseSummary{StatusCode: statusCode, Duration: duration}, err
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		statusCode = http.StatusGatewayTimeout
		http.Error(w, "Gateway Timeout", statusCode)
		return &ResponseSummary{StatusCode: statusCode, Duration: duration}, err
	}

	http.Error(w, "Bad Gateway", statusCode)
	return &ResponseSummary{StatusCode: statusCode, Duration: duration}, err
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		if a == "" || b == "" {
			return a + b
		}
		return a + "/" + b
	}
	return a + b
}
