package rule

import (
	"net"
	"strings"
)

type compiledEndpoint struct {
	rule       EndpointRule
	isWildcard bool
	prefix     string
}

func newCompiledEndpoint(rule EndpointRule) compiledEndpoint {
	isWildcard := strings.HasSuffix(rule.Path, "/*")
	prefix := rule.Path
	if isWildcard {
		prefix = strings.TrimSuffix(rule.Path, "/*")
	}
	return compiledEndpoint{
		rule:       rule,
		isWildcard: isWildcard,
		prefix:     prefix,
	}
}

func (ce *CompiledEndpoint) matches(path, method string) bool {
	// 1. Method check
	if ce.rule.Method != "*" && ce.rule.Method != "" && !strings.EqualFold(ce.rule.Method, method) {
		return false
	}

	// 2. Path check
	if ce.isWildcard {
		return path == ce.prefix || strings.HasPrefix(path, ce.prefix+"/")
	}
	return path == ce.rule.Path
}

// CompiledEndpoint represents an internally optimized path/method matcher.
type CompiledEndpoint = compiledEndpoint

type compiledClient struct {
	rule      ClientRule
	ipNet     *net.IPNet
	singleIP  net.IP
	overrides []compiledEndpoint
}

// Engine evaluates incoming requests against the hierarchical rule matrix.
// It is completely read-only and thread-safe, making it ideal for lock-free atomic pointer swaps.
type Engine struct {
	global    CompiledRule
	endpoints []compiledEndpoint

	byAPIKey []compiledClient
	byUserID []compiledClient
	byIP     []compiledClient
}

// NewEngine compiles a validated Config into an optimized lookup Engine.
func NewEngine(cfg *Config) *Engine {
	e := &Engine{
		global: CompiledRule{
			Scope:    "global",
			RuleID:   "global",
			Capacity: cfg.Global.Capacity,
			Rate:     cfg.Global.Rate,
			Window:   cfg.Global.Window,
		},
		endpoints: make([]compiledEndpoint, len(cfg.Endpoints)),
	}

	for i, ep := range cfg.Endpoints {
		e.endpoints[i] = newCompiledEndpoint(ep)
	}

	for _, c := range cfg.Clients {
		cc := compiledClient{
			rule:      c,
			overrides: make([]compiledEndpoint, len(c.Overrides)),
		}

		for j, o := range c.Overrides {
			cc.overrides[j] = newCompiledEndpoint(o)
		}

		if c.IPNet != "" {
			_, ipNet, err := net.ParseCIDR(c.IPNet)
			if err == nil {
				cc.ipNet = ipNet
			} else {
				cc.singleIP = net.ParseIP(c.IPNet)
			}
			e.byIP = append(e.byIP, cc)
		}

		if c.APIKey != "" {
			e.byAPIKey = append(e.byAPIKey, cc)
		}

		if c.UserID != "" {
			e.byUserID = append(e.byUserID, cc)
		}
	}

	return e
}

// Match determines the winning rate limit rule following strict precedence:
// Tier 1: Client-Specific Endpoint Override
// Tier 2: Client-Specific Global Limit
// Tier 3: Endpoint-Specific Limit
// Tier 4: Global Default Limit
func (e *Engine) Match(req *MatchRequest) *CompiledRule {
	// 1. Resolve client identity
	matchedClient := e.matchClient(req)

	if matchedClient != nil {
		// Tier 1: Check Client-Specific Endpoint Override
		for _, override := range matchedClient.overrides {
			if override.matches(req.Path, req.Method) {
				return &CompiledRule{
					Scope:    "client_endpoint_override",
					RuleID:   matchedClient.rule.ID + ":" + override.rule.Path,
					Capacity: override.rule.Capacity,
					Rate:     override.rule.Rate,
					Window:   override.rule.Window,
				}
			}
		}

		// Tier 2: Client-Specific Global Limit
		return &CompiledRule{
			Scope:    "client_global",
			RuleID:   matchedClient.rule.ID,
			Capacity: matchedClient.rule.Capacity,
			Rate:     matchedClient.rule.Rate,
			Window:   matchedClient.rule.Window,
		}
	}

	// Tier 3: Endpoint-Specific Limit
	for _, ep := range e.endpoints {
		if ep.matches(req.Path, req.Method) {
			return &CompiledRule{
				Scope:    "endpoint",
				RuleID:   ep.rule.Path,
				Capacity: ep.rule.Capacity,
				Rate:     ep.rule.Rate,
				Window:   ep.rule.Window,
			}
		}
	}

	// Tier 4: Global Default Limit
	rule := e.global
	return &rule
}

func (e *Engine) matchClient(req *MatchRequest) *compiledClient {
	// 1. Check API Key
	if req.APIKey != "" {
		for i := range e.byAPIKey {
			if e.byAPIKey[i].rule.APIKey == req.APIKey {
				return &e.byAPIKey[i]
			}
		}
	}

	// 2. Check User ID
	if req.UserID != "" {
		for i := range e.byUserID {
			if e.byUserID[i].rule.UserID == req.UserID {
				return &e.byUserID[i]
			}
		}
	}

	// 3. Check IP / CIDR
	if req.IP != nil {
		for i := range e.byIP {
			cc := &e.byIP[i]
			if cc.ipNet != nil && cc.ipNet.Contains(req.IP) {
				return cc
			}
			if cc.singleIP != nil && cc.singleIP.Equal(req.IP) {
				return cc
			}
		}
	}

	return nil
}
