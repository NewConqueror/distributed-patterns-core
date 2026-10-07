package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestForwarder_BasicForwarding(t *testing.T) {
	// Setup mock upstream server
	upstreamReceivedHeader := make(http.Header)
	var upstreamReceivedBody string
	var upstreamReceivedPath string
	var upstreamReceivedQuery string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamReceivedPath = r.URL.Path
		upstreamReceivedQuery = r.URL.RawQuery
		for k, v := range r.Header {
			upstreamReceivedHeader[k] = v
		}
		bodyBytes, _ := io.ReadAll(r.Body)
		upstreamReceivedBody = string(bodyBytes)

		w.Header().Set("X-Upstream-Header", "hello-from-upstream")
		w.Header().Set("Connection", "close") // hop-by-hop should be stripped
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"created"}`))
	}))
	defer upstream.Close()

	targetURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("failed to parse upstream URL: %v", err)
	}

	forwarder := NewForwarder(nil)

	// Create incoming client request
	reqBody := `{"name":"test-item"}`
	clientReq := httptest.NewRequest(http.MethodPost, "http://gateway.local/api/items?filter=active", strings.NewReader(reqBody))
	clientReq.RemoteAddr = "192.168.1.50:54321"
	clientReq.Header.Set("Content-Type", "application/json")
	clientReq.Header.Set("Connection", "X-Secret-Debug, Keep-Alive")
	clientReq.Header.Set("X-Secret-Debug", "super-secret")
	clientReq.Header.Set("Keep-Alive", "timeout=30")

	recorder := httptest.NewRecorder()

	summary, err := forwarder.Forward(recorder, clientReq, targetURL)
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}

	// 1. Validate upstream received the request properly
	if upstreamReceivedPath != "/api/items" {
		t.Errorf("expected upstream path /api/items, got %s", upstreamReceivedPath)
	}
	if upstreamReceivedQuery != "filter=active" {
		t.Errorf("expected query filter=active, got %s", upstreamReceivedQuery)
	}
	if upstreamReceivedBody != reqBody {
		t.Errorf("expected body %s, got %s", reqBody, upstreamReceivedBody)
	}

	// 2. Validate Hop-by-hop headers were stripped from request to upstream
	if upstreamReceivedHeader.Get("Connection") != "" {
		t.Errorf("expected Connection header to be removed from upstream request")
	}
	if upstreamReceivedHeader.Get("Keep-Alive") != "" {
		t.Errorf("expected Keep-Alive header to be removed from upstream request")
	}
	if upstreamReceivedHeader.Get("X-Secret-Debug") != "" {
		t.Errorf("expected X-Secret-Debug (listed in Connection) to be removed")
	}

	// 3. Validate X-Forwarded-For and X-Real-IP
	if gotIP := upstreamReceivedHeader.Get("X-Real-IP"); gotIP != "192.168.1.50" {
		t.Errorf("expected X-Real-IP to be 192.168.1.50, got %q", gotIP)
	}
	if gotXFF := upstreamReceivedHeader.Get("X-Forwarded-For"); gotXFF != "192.168.1.50" {
		t.Errorf("expected X-Forwarded-For to be 192.168.1.50, got %q", gotXFF)
	}

	// 4. Validate downstream response
	if recorder.Code != http.StatusCreated {
		t.Errorf("expected status %d, got %d", http.StatusCreated, recorder.Code)
	}
	if recorder.Header().Get("X-Upstream-Header") != "hello-from-upstream" {
		t.Errorf("expected X-Upstream-Header to be forwarded")
	}
	// Upstream sent Connection: close, which is hop-by-hop and must not reach downstream
	if recorder.Header().Get("Connection") != "" {
		t.Errorf("expected Connection header to be stripped from response")
	}
	if strings.TrimSpace(recorder.Body.String()) != `{"status":"created"}` {
		t.Errorf("expected response body %s, got %s", `{"status":"created"}`, recorder.Body.String())
	}

	// 5. Validate summary
	if summary.StatusCode != http.StatusCreated {
		t.Errorf("expected summary status %d, got %d", http.StatusCreated, summary.StatusCode)
	}
	if summary.BytesWritten != int64(len(`{"status":"created"}`)) {
		t.Errorf("expected summary bytes %d, got %d", len(`{"status":"created"}`), summary.BytesWritten)
	}
}

func TestForwarder_XForwardedForAppend(t *testing.T) {
	var upstreamXFF string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamXFF = r.Header.Get("X-Forwarded-For")
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	targetURL, _ := url.Parse(upstream.URL)
	forwarder := NewForwarder(nil)

	clientReq := httptest.NewRequest(http.MethodGet, "/test", nil)
	clientReq.RemoteAddr = "10.0.0.5:12345"
	clientReq.Header.Set("X-Forwarded-For", "203.0.113.195")

	recorder := httptest.NewRecorder()
	_, err := forwarder.Forward(recorder, clientReq, targetURL)
	if err != nil {
		t.Fatalf("forward error: %v", err)
	}

	expectedXFF := "203.0.113.195, 10.0.0.5"
	if upstreamXFF != expectedXFF {
		t.Errorf("expected X-Forwarded-For to be %q, got %q", expectedXFF, upstreamXFF)
	}
}

func TestForwarder_UpstreamUnreachable(t *testing.T) {
	forwarder := NewForwarder(nil)
	deadURL, _ := url.Parse("http://127.0.0.1:54321") // Non-existent port

	clientReq := httptest.NewRequest(http.MethodGet, "/unreachable", nil)
	clientReq.RemoteAddr = "127.0.0.1:50000"

	recorder := httptest.NewRecorder()
	summary, err := forwarder.Forward(recorder, clientReq, deadURL)
	if err == nil {
		t.Fatal("expected error for unreachable upstream, got nil")
	}

	if recorder.Code != http.StatusBadGateway {
		t.Errorf("expected status %d for unreachable upstream, got %d", http.StatusBadGateway, recorder.Code)
	}
	if summary.StatusCode != http.StatusBadGateway {
		t.Errorf("expected summary status %d, got %d", http.StatusBadGateway, summary.StatusCode)
	}
}

func TestForwarder_ContextTimeout(t *testing.T) {
	slowUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slowUpstream.Close()

	targetURL, _ := url.Parse(slowUpstream.URL)
	forwarder := NewForwarder(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	clientReq := httptest.NewRequest(http.MethodGet, "/slow", nil).WithContext(ctx)
	clientReq.RemoteAddr = "127.0.0.1:50000"

	recorder := httptest.NewRecorder()
	summary, err := forwarder.Forward(recorder, clientReq, targetURL)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if recorder.Code != http.StatusGatewayTimeout {
		t.Errorf("expected 504 Gateway Timeout, got %d", recorder.Code)
	}
	if summary.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("expected summary status 504, got %d", summary.StatusCode)
	}
}
