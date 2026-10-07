package rule

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// LimitRule defines the rate-limiting limits for a matched scope.
type LimitRule struct {
	Capacity int64         `json:"capacity" yaml:"capacity"`
	Rate     float64       `json:"rate" yaml:"rate"`
	Window   time.Duration `json:"window" yaml:"window"`
}

// EndpointRule defines rate-limiting rules specific to an HTTP path and optional method.
type EndpointRule struct {
	Path     string        `json:"path" yaml:"path"`
	Method   string        `json:"method" yaml:"method"` // GET, POST, or "*" for any
	Capacity int64         `json:"capacity" yaml:"capacity"`
	Rate     float64       `json:"rate" yaml:"rate"`
	Window   time.Duration `json:"window" yaml:"window"`
}

// ClientRule defines tier limits for an identified client (via API Key, IP/CIDR, or User ID).
type ClientRule struct {
	ID        string         `json:"id" yaml:"id"`
	APIKey    string         `json:"api_key" yaml:"api_key"`
	IPNet     string         `json:"ip_net" yaml:"ip_net"`   // Specific IP or CIDR (e.g. 192.168.1.0/24)
	UserID    string         `json:"user_id" yaml:"user_id"` // User identifier from auth
	Capacity  int64          `json:"capacity" yaml:"capacity"`
	Rate      float64        `json:"rate" yaml:"rate"`
	Window    time.Duration  `json:"window" yaml:"window"`
	Overrides []EndpointRule `json:"overrides" yaml:"overrides"` // Client-specific endpoint overrides
}

// Config represents the raw unparsed or parsed rule specification file.
type Config struct {
	Global    LimitRule      `json:"global" yaml:"global"`
	Endpoints []EndpointRule `json:"endpoints" yaml:"endpoints"`
	Clients   []ClientRule   `json:"clients" yaml:"clients"`
}

// CompiledRule represents the final resolved rule applying to an incoming request.
type CompiledRule struct {
	Scope    string        // "client_endpoint_override", "client_global", "endpoint", "global"
	RuleID   string        // Identifying label for logging/metrics
	Capacity int64
	Rate     float64
	Window   time.Duration
}

// MatchRequest gathers metadata needed to evaluate rule hierarchy against an HTTP request.
type MatchRequest struct {
	Path   string
	Method string
	APIKey string
	IP     net.IP
	UserID string
}

// NewMatchRequest creates a MatchRequest from an HTTP request and extracted client metadata.
func NewMatchRequest(r *http.Request, apiKey, clientIP, userID string) *MatchRequest {
	var ip net.IP
	if clientIP != "" {
		ip = net.ParseIP(clientIP)
	}

	return &MatchRequest{
		Path:   r.URL.Path,
		Method: strings.ToUpper(r.Method),
		APIKey: apiKey,
		IP:     ip,
		UserID: userID,
	}
}

// Key returns a unique rate-limiting counter key for this rule and request combination.
func (c *CompiledRule) CounterKey(req *MatchRequest) string {
	switch c.Scope {
	case "client_endpoint_override":
		return fmt.Sprintf("rl:c_ep:{%s}:%s", c.RuleID, req.Path)
	case "client_global":
		return fmt.Sprintf("rl:client:{%s}", c.RuleID)
	case "endpoint":
		return fmt.Sprintf("rl:ep:{%s}:%s", req.Path, c.clientKey(req))
	default:
		return fmt.Sprintf("rl:global:{%s}", c.clientKey(req))
	}
}

func (c *CompiledRule) clientKey(req *MatchRequest) string {
	if req.APIKey != "" {
		return "key:" + req.APIKey
	}
	if req.UserID != "" {
		return "user:" + req.UserID
	}
	if req.IP != nil {
		return "ip:" + req.IP.String()
	}
	return "anonymous"
}
