package safetool

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/tool"
)

type memoryAuditLogger struct {
	logs []AuditLog
}

func (m *memoryAuditLogger) Log(entry AuditLog) {
	m.logs = append(m.logs, entry)
}

func setupTestExecutor() (*Executor, *memoryAuditLogger) {
	reg := tool.NewRegistry()

	// 1. Safe read-only tool
	_ = reg.Register(tool.Tool{
		Definition: tool.Definition{
			Name:        "get_order",
			Description: "Get order details",
			ReadOnly:    true,
			Parameters: tool.JSONSchema{
				Type:     tool.TypeObject,
				Required: []string{"id"},
				Properties: map[string]tool.JSONSchema{
					"id": {Type: tool.TypeString},
				},
			},
			RequiredPermissions: []string{"orders:read"},
		},
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			return map[string]string{"order_id": args["id"].(string), "status": "delivered"}, nil
		},
	})

	// 2. Mutating tool
	_ = reg.Register(tool.Tool{
		Definition: tool.Definition{
			Name:        "cancel_order",
			Description: "Cancel an order",
			ReadOnly:    false,
			Parameters: tool.JSONSchema{
				Type:     tool.TypeObject,
				Required: []string{"id"},
				Properties: map[string]tool.JSONSchema{
					"id": {Type: tool.TypeString},
				},
			},
			RequiredPermissions: []string{"orders:write"},
		},
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			return map[string]string{"canceled": "true"}, nil
		},
	})

	// 3. Hanging tool
	_ = reg.Register(tool.Tool{
		Definition: tool.Definition{
			Name:     "slow_tool",
			ReadOnly: true,
			Parameters: tool.JSONSchema{
				Type: tool.TypeObject,
			},
		},
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			select {
			case <-time.After(500 * time.Millisecond):
				return "done", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	})

	// 4. Panicking tool
	_ = reg.Register(tool.Tool{
		Definition: tool.Definition{
			Name:     "panic_tool",
			ReadOnly: true,
			Parameters: tool.JSONSchema{
				Type: tool.TypeObject,
			},
		},
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			panic("unexpected null pointer dereference")
		},
	})

	logger := &memoryAuditLogger{}
	exec := NewExecutor(ExecutorConfig{
		Registry:    reg,
		AuditLogger: logger,
		Timeout:     50 * time.Millisecond, // fast timeout for test
	})

	return exec, logger
}

func TestExecutor_Success(t *testing.T) {
	exec, logger := setupTestExecutor()

	ctx := WithCaller(context.Background(), CallerIdentity{
		ID:          "user-1",
		Permissions: []string{"orders:read"},
	})

	res, err := exec.Execute(ctx, "get_order", map[string]any{"id": "ORD-123"})
	if err != nil {
		t.Fatalf("unexpected execution error: %v", err)
	}

	resMap := res.(map[string]string)
	if resMap["status"] != "delivered" {
		t.Fatalf("expected status delivered, got %s", resMap["status"])
	}

	if len(logger.logs) != 1 {
		t.Fatalf("expected 1 audit log, got %d", len(logger.logs))
	}
	if logger.logs[0].Status != "success" {
		t.Fatalf("expected audit log status 'success', got %s", logger.logs[0].Status)
	}
}

func TestExecutor_RBAC_Unauthorized(t *testing.T) {
	exec, logger := setupTestExecutor()

	// Caller only has orders:read, but calls cancel_order (needs orders:write)
	ctx := WithCaller(context.Background(), CallerIdentity{
		ID:          "user-2",
		Permissions: []string{"orders:read"},
	})

	_, err := exec.Execute(ctx, "cancel_order", map[string]any{"id": "ORD-123"})
	if err == nil {
		t.Fatal("expected unauthorized error, got nil")
	}

	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}

	if len(logger.logs) != 1 || logger.logs[0].Status != "unauthorized" {
		t.Fatalf("expected audit log to record unauthorized, got %+v", logger.logs)
	}
}

func TestExecutor_ReadOnlyEnforcement(t *testing.T) {
	exec, logger := setupTestExecutor()

	// Caller is marked ReadOnly=true, tries to execute mutating cancel_order
	ctx := WithCaller(context.Background(), CallerIdentity{
		ID:          "user-3",
		Permissions: []string{"orders:write"},
		ReadOnly:    true,
	})

	_, err := exec.Execute(ctx, "cancel_order", map[string]any{"id": "ORD-123"})
	if err == nil {
		t.Fatal("expected read-only violation error, got nil")
	}

	if !errors.Is(err, ErrReadOnlyViolation) {
		t.Fatalf("expected ErrReadOnlyViolation, got %v", err)
	}
	if logger.logs[0].Status != "unauthorized" {
		t.Fatalf("expected audit log status unauthorized, got %s", logger.logs[0].Status)
	}
}

func TestExecutor_RawSQLInjectionBlocked(t *testing.T) {
	exec, _ := setupTestExecutor()

	ctx := WithCaller(context.Background(), CallerIdentity{
		ID:    "user-4",
		Roles: []string{"admin"},
	})

	// Malicious prompt argument containing raw SQL injection
	_, err := exec.Execute(ctx, "get_order", map[string]any{
		"id": "1; DROP TABLE users; --",
	})
	if err == nil {
		t.Fatal("expected raw SQL injection error, got nil")
	}

	if !errors.Is(err, ErrRawSQLProhibited) {
		t.Fatalf("expected ErrRawSQLProhibited, got %v", err)
	}
}

func TestExecutor_TimeoutEnforcement(t *testing.T) {
	exec, logger := setupTestExecutor()

	ctx := WithCaller(context.Background(), CallerIdentity{
		ID:    "user-5",
		Roles: []string{"admin"},
	})

	// slow_tool takes 500ms, timeout configured to 50ms
	_, err := exec.Execute(ctx, "slow_tool", map[string]any{})
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !errors.Is(err, ErrToolTimeout) {
		t.Fatalf("expected ErrToolTimeout, got %v", err)
	}

	if len(logger.logs) != 1 || logger.logs[0].Status != "timeout" {
		t.Fatalf("expected audit log status 'timeout', got %+v", logger.logs)
	}
}

func TestExecutor_PanicRecovery(t *testing.T) {
	exec, logger := setupTestExecutor()

	ctx := WithCaller(context.Background(), CallerIdentity{
		ID:    "user-6",
		Roles: []string{"admin"},
	})

	_, err := exec.Execute(ctx, "panic_tool", map[string]any{})
	if err == nil {
		t.Fatal("expected panic error, got nil")
	}

	if !errors.Is(err, ErrToolPanicked) {
		t.Fatalf("expected ErrToolPanicked, got %v", err)
	}

	if len(logger.logs) != 1 || logger.logs[0].Status != "failed" {
		t.Fatalf("expected audit log status 'failed', got %+v", logger.logs)
	}
}

func TestJSONAuditLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewJSONAuditLogger(&buf)

	logger.Log(AuditLog{
		ExecutionID: "exec-test-1",
		ToolName:    "test_tool",
		Status:      "success",
		DurationMS:  1.5,
	})

	output := buf.String()
	if !strings.Contains(output, "exec-test-1") || !strings.Contains(output, "test_tool") {
		t.Fatalf("unexpected audit log output: %s", output)
	}
}
