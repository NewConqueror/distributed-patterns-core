package proxy

import (
	"net/http"
	"testing"
)

func createHeader(kvs map[string]string) http.Header {
	h := make(http.Header)
	for k, v := range kvs {
		h.Set(k, v)
	}
	return h
}

func TestRemoveHopByHopHeaders(t *testing.T) {
	tests := []struct {
		name     string
		input    http.Header
		expected http.Header
	}{
		{
			name: "Standard hop-by-hop headers removal",
			input: createHeader(map[string]string{
				"Connection":          "close",
				"Keep-Alive":          "timeout=5",
				"Proxy-Authenticate":  "Basic",
				"Proxy-Authorization": "Bearer xyz",
				"Upgrade":             "websocket",
				"Transfer-Encoding":   "chunked",
				"Trailer":             "Expires",
				"Content-Type":        "application/json",
				"Authorization":       "Bearer token123",
			}),
			expected: createHeader(map[string]string{
				"Content-Type":  "application/json",
				"Authorization": "Bearer token123",
			}),
		},
		{
			name: "Custom headers listed in Connection header",
			input: createHeader(map[string]string{
				"Connection":       "X-Foo-Bar, X-Another-Custom",
				"X-Foo-Bar":        "some-value",
				"X-Another-Custom": "another-value",
				"X-End-To-End":     "keep-me",
				"Content-Length":   "42",
			}),
			expected: createHeader(map[string]string{
				"X-End-To-End":   "keep-me",
				"Content-Length": "42",
			}),
		},
		{
			name: "TE header with trailers is preserved",
			input: createHeader(map[string]string{
				"TE":           "trailers",
				"Content-Type": "text/plain",
			}),
			expected: createHeader(map[string]string{
				"TE":           "trailers",
				"Content-Type": "text/plain",
			}),
		},
		{
			name: "TE header with non-trailers is stripped",
			input: createHeader(map[string]string{
				"TE":           "deflate",
				"Content-Type": "text/plain",
			}),
			expected: createHeader(map[string]string{
				"Content-Type": "text/plain",
			}),
		},
		{
			name:     "Nil header map does not panic",
			input:    nil,
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			RemoveHopByHopHeaders(tc.input)
			if tc.input == nil {
				return
			}
			for k := range tc.expected {
				got := tc.input.Values(k)
				want := tc.expected.Values(k)
				if len(got) != len(want) {
					t.Fatalf("expected header %s length %d, got %d", k, len(want), len(got))
				}
				for i := range want {
					if got[i] != want[i] {
						t.Errorf("header %s: expected %q, got %q", k, want[i], got[i])
					}
				}
			}
			for k := range tc.input {
				if len(tc.expected.Values(k)) == 0 {
					t.Errorf("unexpected header remaining: %s = %v", k, tc.input.Values(k))
				}
			}
		})
	}
}
