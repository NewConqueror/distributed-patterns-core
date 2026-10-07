package safetool

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/tool"
)

var (
	// ErrUnauthorized is returned when caller lacks required permissions for a tool.
	ErrUnauthorized = errors.New("unauthorized: caller lacks required permissions for this tool")
	// ErrReadOnlyViolation is returned when a mutating tool is called in read-only mode.
	ErrReadOnlyViolation = errors.New("security violation: mutating tool cannot be executed in read-only mode")
	// ErrRawSQLProhibited is returned when free-form SQL statements are detected in arguments.
	ErrRawSQLProhibited = errors.New("security violation: raw SQL queries are prohibited; only parameterized safe tools are allowed")
)

// CallerIdentity encapsulates the identity and granted roles/permissions of a caller.
type CallerIdentity struct {
	ID          string   `json:"id"`
	APIKey      string   `json:"api_key,omitempty"`
	Roles       []string `json:"roles,omitempty"`
	Permissions []string `json:"permissions,omitempty"`
	ReadOnly    bool     `json:"read_only"`
}

// HasPermission checks if the caller possesses the requested permission or admin role.
func (c *CallerIdentity) HasPermission(perm string) bool {
	for _, r := range c.Roles {
		if r == "admin" || r == "*" {
			return true
		}
	}
	for _, p := range c.Permissions {
		if p == perm || p == "*" {
			return true
		}
	}
	return false
}

// PolicyEngine enforces RBAC, read-only policies, and sanitizes arguments.
type PolicyEngine struct {
	strictSQLCheck bool
}

// NewPolicyEngine creates a new PolicyEngine.
func NewPolicyEngine() *PolicyEngine {
	return &PolicyEngine{
		strictSQLCheck: true,
	}
}

// Authorize checks whether the caller is authorized to execute the tool under policy constraints.
func (p *PolicyEngine) Authorize(caller CallerIdentity, t tool.Definition, args map[string]any) error {
	// 1. Read-Only Enforcer
	if caller.ReadOnly && !t.ReadOnly {
		return fmt.Errorf("%w: tool %q is not read-only", ErrReadOnlyViolation, t.Name)
	}

	// 2. RBAC (Role-Based Access Control)
	for _, reqPerm := range t.RequiredPermissions {
		if !caller.HasPermission(reqPerm) {
			return fmt.Errorf("%w: requires permission %q", ErrUnauthorized, reqPerm)
		}
	}

	// 3. Raw SQL Injection Prevention in arguments
	if p.strictSQLCheck {
		if err := p.inspectRawSQL(args); err != nil {
			return err
		}
	}

	return nil
}

// Regex to catch free-form raw SQL injection attempts in string arguments
var rawSQLRegex = regexp.MustCompile(`(?i)\b(SELECT\s+.+\s+FROM|DROP\s+TABLE|DELETE\s+FROM|INSERT\s+INTO|UPDATE\s+.+\s+SET|UNION\s+ALL|UNION\s+SELECT|ALTER\s+TABLE|EXEC\s*\(|EXECUTE\s*\(|--|;)\b`)

func (p *PolicyEngine) inspectRawSQL(args map[string]any) error {
	for k, v := range args {
		switch val := v.(type) {
		case string:
			if rawSQLRegex.MatchString(val) {
				return fmt.Errorf("%w: dangerous SQL pattern detected in parameter %q", ErrRawSQLProhibited, k)
			}
		case map[string]any:
			if err := p.inspectRawSQL(val); err != nil {
				return err
			}
		}
	}
	return nil
}

// ContextKey for attaching CallerIdentity to context.Context
type contextKey struct{}

var callerContextKey = contextKey{}

// WithCaller injects CallerIdentity into context.
func WithCaller(ctx context.Context, caller CallerIdentity) context.Context {
	return context.WithValue(ctx, callerContextKey, caller)
}

// CallerFromContext extracts CallerIdentity from context.
func CallerFromContext(ctx context.Context) (CallerIdentity, bool) {
	val, ok := ctx.Value(callerContextKey).(CallerIdentity)
	return val, ok
}
