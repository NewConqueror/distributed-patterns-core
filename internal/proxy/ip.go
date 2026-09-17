package proxy

import (
	"net"
	"net/http"
)

// ExtractClientIP extracts the IP address from a request's RemoteAddr (ignoring port).
// If RemoteAddr is invalid or empty, it returns the raw value or fallback.
func ExtractClientIP(r *http.Request) string {
	if r.RemoteAddr == "" {
		return ""
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// SetForwardingHeaders enriches the outbound request headers with standard proxy forwarding metadata:
// - X-Forwarded-For: appends client IP to the existing chain, or initializes it.
// - X-Real-IP: sets the immediate client IP address.
// - X-Forwarded-Proto: sets "https" if TLS was negotiated, otherwise "http".
// - X-Forwarded-Host: sets the original Host requested by the client.
func SetForwardingHeaders(req *http.Request, clientIP string) {
	if clientIP == "" {
		clientIP = ExtractClientIP(req)
	}

	// 1. X-Forwarded-For
	if clientIP != "" {
		if prior := req.Header.Get("X-Forwarded-For"); prior != "" {
			req.Header.Set("X-Forwarded-For", prior+", "+clientIP)
		} else {
			req.Header.Set("X-Forwarded-For", clientIP)
		}

		// 2. X-Real-IP
		req.Header.Set("X-Real-IP", clientIP)
	}

	// 3. X-Forwarded-Proto
	if req.Header.Get("X-Forwarded-Proto") == "" {
		if req.TLS != nil {
			req.Header.Set("X-Forwarded-Proto", "https")
		} else {
			req.Header.Set("X-Forwarded-Proto", "http")
		}
	}

	// 4. X-Forwarded-Host
	if req.Header.Get("X-Forwarded-Host") == "" && req.Host != "" {
		req.Header.Set("X-Forwarded-Host", req.Host)
	}
}
