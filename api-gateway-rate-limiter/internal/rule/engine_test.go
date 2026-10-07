package rule

import (
	"net"
	"net/http"
	"testing"
	"time"
)

var sampleYAML = `
global:
  capacity: 100
  rate: 10.0
  window: 60s

endpoints:
  - path: "/api/v1/checkout"
    method: "POST"
    capacity: 5
    rate: 1.0
    window: 10s
  - path: "/api/v1/auth/*"
    method: "*"
    capacity: 15
    rate: 3.0
    window: 30s

clients:
  - id: "vip-partner"
    api_key: "vip-secret-123"
    capacity: 1000
    rate: 100.0
    window: 60s
    overrides:
      - path: "/api/v1/checkout"
        method: "POST"
        capacity: 50
        rate: 5.0
        window: 10s
  - id: "internal-network"
    ip_net: "10.0.0.0/8"
    capacity: 500
    rate: 50.0
    window: 60s
`

func TestValidator_ValidYAML(t *testing.T) {
	cfg, err := ParseConfig([]byte(sampleYAML))
	if err != nil {
		t.Fatalf("failed to parse valid YAML: %v", err)
	}

	if cfg.Global.Capacity != 100 {
		t.Errorf("expected global capacity 100, got %d", cfg.Global.Capacity)
	}
	if len(cfg.Endpoints) != 2 {
		t.Errorf("expected 2 endpoints, got %d", len(cfg.Endpoints))
	}
	if len(cfg.Clients) != 2 {
		t.Errorf("expected 2 clients, got %d", len(cfg.Clients))
	}
}

func TestValidator_InvalidConfigs(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "Zero capacity global",
			yaml: `global: { capacity: 0, window: 10s }`,
		},
		{
			name: "Negative window",
			yaml: `global: { capacity: 10, window: -5s }`,
		},
		{
			name: "Endpoint path without leading slash",
			yaml: `
global: { capacity: 10, window: 10s }
endpoints: [{ path: "invalid/path", capacity: 5, window: 10s }]`,
		},
		{
			name: "Client without identifier",
			yaml: `
global: { capacity: 10, window: 10s }
clients: [{ id: "bad-client", capacity: 5, window: 10s }]`,
		},
		{
			name: "Duplicate client id",
			yaml: `
global: { capacity: 10, window: 10s }
clients:
  - id: "dup"
    api_key: "key1"
    capacity: 5
    window: 10s
  - id: "dup"
    api_key: "key2"
    capacity: 5
    window: 10s`,
		},
		{
			name: "Invalid IP CIDR",
			yaml: `
global: { capacity: 10, window: 10s }
clients:
  - id: "bad-ip"
    ip_net: "999.999.999.999/99"
    capacity: 5
    window: 10s`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseConfig([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("expected validation error for %s, got nil", tc.name)
			}
		})
	}
}

func TestEngine_PrecedenceHierarchy(t *testing.T) {
	cfg, err := ParseConfig([]byte(sampleYAML))
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	engine := NewEngine(cfg)

	// 1. Tier 1: VIP partner on /api/v1/checkout (POST) -> Client Endpoint Override (Capacity 50)
	req1 := &MatchRequest{
		Path:   "/api/v1/checkout",
		Method: "POST",
		APIKey: "vip-secret-123",
	}
	rule1 := engine.Match(req1)
	if rule1.Scope != "client_endpoint_override" {
		t.Errorf("expected scope client_endpoint_override, got %s", rule1.Scope)
	}
	if rule1.Capacity != 50 {
		t.Errorf("expected capacity 50, got %d", rule1.Capacity)
	}

	// 2. Tier 2: VIP partner on /api/v1/users (GET) -> Client Global (Capacity 1000)
	req2 := &MatchRequest{
		Path:   "/api/v1/users",
		Method: "GET",
		APIKey: "vip-secret-123",
	}
	rule2 := engine.Match(req2)
	if rule2.Scope != "client_global" {
		t.Errorf("expected scope client_global, got %s", rule2.Scope)
	}
	if rule2.Capacity != 1000 {
		t.Errorf("expected capacity 1000, got %d", rule2.Capacity)
	}

	// 3. Tier 3: Anonymous request to /api/v1/checkout (POST) -> Endpoint Rule (Capacity 5)
	req3 := &MatchRequest{
		Path:   "/api/v1/checkout",
		Method: "POST",
	}
	rule3 := engine.Match(req3)
	if rule3.Scope != "endpoint" {
		t.Errorf("expected scope endpoint, got %s", rule3.Scope)
	}
	if rule3.Capacity != 5 {
		t.Errorf("expected capacity 5, got %d", rule3.Capacity)
	}

	// 4. Tier 3: Anonymous request to /api/v1/auth/login (POST) -> Wildcard Endpoint Rule (Capacity 15)
	req4 := &MatchRequest{
		Path:   "/api/v1/auth/login",
		Method: "POST",
	}
	rule4 := engine.Match(req4)
	if rule4.Scope != "endpoint" {
		t.Errorf("expected scope endpoint, got %s", rule4.Scope)
	}
	if rule4.Capacity != 15 {
		t.Errorf("expected capacity 15, got %d", rule4.Capacity)
	}

	// 5. Tier 4: Anonymous request to /api/v1/other (GET) -> Global Rule (Capacity 100)
	req5 := &MatchRequest{
		Path:   "/api/v1/other",
		Method: "GET",
	}
	rule5 := engine.Match(req5)
	if rule5.Scope != "global" {
		t.Errorf("expected scope global, got %s", rule5.Scope)
	}
	if rule5.Capacity != 100 {
		t.Errorf("expected capacity 100, got %d", rule5.Capacity)
	}
}

func TestEngine_IPMatching(t *testing.T) {
	cfg, err := ParseConfig([]byte(sampleYAML))
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	engine := NewEngine(cfg)

	// IP 10.24.1.5 falls inside 10.0.0.0/8 -> internal-network (Capacity 500)
	req := &MatchRequest{
		Path:   "/any/path",
		Method: "GET",
		IP:     net.ParseIP("10.24.1.5"),
	}
	rule := engine.Match(req)
	if rule.Scope != "client_global" || rule.RuleID != "internal-network" {
		t.Errorf("expected client_global with id internal-network, got %s (%s)", rule.Scope, rule.RuleID)
	}
	if rule.Capacity != 500 {
		t.Errorf("expected capacity 500, got %d", rule.Capacity)
	}
}

func TestNewMatchRequest(t *testing.T) {
	httpReq, _ := http.NewRequest(http.MethodPost, "http://api.example.com/api/v1/checkout", nil)
	mr := NewMatchRequest(httpReq, "api-123", "192.168.1.100", "user-456")

	if mr.Path != "/api/v1/checkout" {
		t.Errorf("expected path /api/v1/checkout, got %s", mr.Path)
	}
	if mr.Method != "POST" {
		t.Errorf("expected method POST, got %s", mr.Method)
	}
	if mr.APIKey != "api-123" {
		t.Errorf("expected APIKey api-123, got %s", mr.APIKey)
	}
	if mr.UserID != "user-456" {
		t.Errorf("expected UserID user-456, got %s", mr.UserID)
	}
	if mr.IP.String() != "192.168.1.100" {
		t.Errorf("expected IP 192.168.1.100, got %s", mr.IP.String())
	}
}

func TestCompiledRule_CounterKey(t *testing.T) {
	mr := &MatchRequest{
		Path:   "/api/v1/checkout",
		Method: "POST",
		APIKey: "vip-key",
	}

	cr := &CompiledRule{
		Scope:    "client_endpoint_override",
		RuleID:   "vip-partner",
		Capacity: 50,
		Window:   time.Minute,
	}

	key := cr.CounterKey(mr)
	if key != "rl:c_ep:{vip-partner}:/api/v1/checkout" {
		t.Errorf("unexpected counter key: %s", key)
	}
}
