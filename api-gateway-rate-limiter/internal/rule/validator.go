package rule

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path"
	"strings"

	"gopkg.in/yaml.v3"
)

var validHTTPMethods = map[string]struct{}{
	"*":       {},
	"":        {},
	"GET":     {},
	"POST":    {},
	"PUT":     {},
	"DELETE":  {},
	"PATCH":   {},
	"HEAD":    {},
	"OPTIONS": {},
}

// ParseConfig decodes raw JSON or YAML bytes into a validated Config object.
func ParseConfig(data []byte) (*Config, error) {
	if len(data) == 0 {
		return nil, errors.New("configuration content is empty")
	}

	var cfg Config
	// Try JSON first if it starts with {
	trimmed := strings.TrimSpace(string(data))
	if strings.HasPrefix(trimmed, "{") {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("invalid JSON configuration: %w", err)
		}
	} else {
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("invalid YAML configuration: %w", err)
		}
	}

	if err := ValidateConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// ValidateConfig validates all tiers of the configuration schema.
func ValidateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("configuration is nil")
	}

	// 1. Validate Global rule
	if err := validateLimitRule(&cfg.Global, "global"); err != nil {
		return err
	}

	// 2. Validate Endpoints
	seenEndpoints := make(map[string]struct{})
	for i := range cfg.Endpoints {
		ep := &cfg.Endpoints[i]
		key := fmt.Sprintf("%s:%s", strings.ToUpper(ep.Method), ep.Path)
		if _, exists := seenEndpoints[key]; exists {
			return fmt.Errorf("duplicate endpoint definition: method=%q path=%q", ep.Method, ep.Path)
		}
		seenEndpoints[key] = struct{}{}

		if err := validateEndpointRule(ep, fmt.Sprintf("endpoints[%d]", i)); err != nil {
			return err
		}
	}

	// 3. Validate Clients
	seenClientIDs := make(map[string]struct{})
	seenAPIKeys := make(map[string]struct{})
	for i := range cfg.Clients {
		c := &cfg.Clients[i]
		if c.ID == "" {
			return fmt.Errorf("clients[%d]: client 'id' cannot be empty", i)
		}
		if _, exists := seenClientIDs[c.ID]; exists {
			return fmt.Errorf("clients[%d]: duplicate client id %q", i, c.ID)
		}
		seenClientIDs[c.ID] = struct{}{}

		if c.APIKey == "" && c.IPNet == "" && c.UserID == "" {
			return fmt.Errorf("client %q must specify at least one identifier (api_key, ip_net, or user_id)", c.ID)
		}

		if c.APIKey != "" {
			if _, exists := seenAPIKeys[c.APIKey]; exists {
				return fmt.Errorf("client %q: duplicate api_key %q", c.ID, c.APIKey)
			}
			seenAPIKeys[c.APIKey] = struct{}{}
		}

		if c.IPNet != "" {
			if _, _, err := net.ParseCIDR(c.IPNet); err != nil {
				if ip := net.ParseIP(c.IPNet); ip == nil {
					return fmt.Errorf("client %q: invalid ip_net %q (must be valid IP or CIDR)", c.ID, c.IPNet)
				}
			}
		}

		limit := LimitRule{Capacity: c.Capacity, Rate: c.Rate, Window: c.Window}
		if err := validateLimitRule(&limit, fmt.Sprintf("client %q", c.ID)); err != nil {
			return err
		}
		c.Capacity = limit.Capacity
		c.Rate = limit.Rate
		c.Window = limit.Window

		// Validate overrides
		seenOverrides := make(map[string]struct{})
		for j := range c.Overrides {
			override := &c.Overrides[j]
			key := fmt.Sprintf("%s:%s", strings.ToUpper(override.Method), override.Path)
			if _, exists := seenOverrides[key]; exists {
				return fmt.Errorf("client %q: duplicate override definition: method=%q path=%q", c.ID, override.Method, override.Path)
			}
			seenOverrides[key] = struct{}{}

			if err := validateEndpointRule(override, fmt.Sprintf("client %q overrides[%d]", c.ID, j)); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateLimitRule(lr *LimitRule, scope string) error {
	if lr.Capacity <= 0 {
		return fmt.Errorf("%s: capacity must be greater than 0, got %d", scope, lr.Capacity)
	}
	if lr.Window <= 0 {
		return fmt.Errorf("%s: window duration must be greater than 0, got %v", scope, lr.Window)
	}
	if lr.Rate <= 0 {
		// Calculate default replenishment rate (tokens per second)
		lr.Rate = float64(lr.Capacity) / lr.Window.Seconds()
	}
	return nil
}

func validateEndpointRule(ep *EndpointRule, scope string) error {
	if ep.Path == "" {
		return fmt.Errorf("%s: path cannot be empty", scope)
	}
	if !strings.HasPrefix(ep.Path, "/") {
		return fmt.Errorf("%s: path must start with '/', got %q", scope, ep.Path)
	}

	ep.Method = strings.ToUpper(strings.TrimSpace(ep.Method))
	if _, ok := validHTTPMethods[ep.Method]; !ok {
		return fmt.Errorf("%s: invalid HTTP method %q", scope, ep.Method)
	}
	if ep.Method == "" {
		ep.Method = "*"
	}

	// Validate path syntax (clean path)
	cleaned := path.Clean(strings.TrimSuffix(ep.Path, "/*"))
	if cleaned == "" {
		return fmt.Errorf("%s: invalid path syntax %q", scope, ep.Path)
	}

	limit := LimitRule{Capacity: ep.Capacity, Rate: ep.Rate, Window: ep.Window}
	if err := validateLimitRule(&limit, scope); err != nil {
		return err
	}
	ep.Capacity = limit.Capacity
	ep.Rate = limit.Rate
	ep.Window = limit.Window

	return nil
}
