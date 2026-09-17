package proxy

import (
	"net"
	"net/http"
	"time"
)

// TransportConfig defines connection pool and timeout settings for upstream communication.
type TransportConfig struct {
	DialTimeout           time.Duration
	KeepAlive             time.Duration
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	IdleConnTimeout       time.Duration
	ResponseHeaderTimeout time.Duration
	ExpectContinueTimeout time.Duration
	TLSHandshakeTimeout   time.Duration
}

// DefaultTransportConfig provides production-ready connection pool and timeout defaults.
func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		DialTimeout:           5 * time.Second,
		KeepAlive:             30 * time.Second,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   100, // Significantly higher than Go's default of 2 to avoid reconnect storms
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
	}
}

// NewPooledTransport creates an http.Transport optimized for reverse proxying with keep-alive connection pooling.
func NewPooledTransport(cfg TransportConfig) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: cfg.KeepAlive,
	}

	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		ExpectContinueTimeout: cfg.ExpectContinueTimeout,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ForceAttemptHTTP2:     true,
	}
}
