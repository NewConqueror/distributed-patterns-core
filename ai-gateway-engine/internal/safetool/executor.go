package safetool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/tool"
)

var (
	// ErrToolTimeout is returned when tool execution exceeds the configured timeout threshold.
	ErrToolTimeout = errors.New("tool execution timed out")
	// ErrToolPanicked is returned when a tool handler panics.
	ErrToolPanicked = errors.New("tool execution panicked")
)

// AuditLog captures structured metadata for a tool execution.
type AuditLog struct {
	ExecutionID string         `json:"execution_id"`
	Timestamp   time.Time      `json:"timestamp"`
	CallerID    string         `json:"caller_id"`
	ToolName    string         `json:"tool_name"`
	Arguments   map[string]any `json:"arguments"`
	Output      any            `json:"output,omitempty"`
	DurationMS  float64        `json:"duration_ms"`
	Status      string         `json:"status"` // "success", "schema_error", "unauthorized", "timeout", "failed"
	Error       string         `json:"error,omitempty"`
}

// AuditLogger records execution logs.
type AuditLogger interface {
	Log(entry AuditLog)
}

// JSONAuditLogger writes formatted JSON audit logs to an io.Writer.
type JSONAuditLogger struct {
	mu     sync.Mutex
	writer io.Writer
}

func NewJSONAuditLogger(w io.Writer) *JSONAuditLogger {
	if w == nil {
		w = os.Stdout
	}
	return &JSONAuditLogger{
		writer: w,
	}
}

func (l *JSONAuditLogger) Log(entry AuditLog) {
	l.mu.Lock()
	defer l.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		log.Printf("[AuditLog] Failed to marshal audit log: %v", err)
		return
	}
	_, _ = l.writer.Write(append(data, '\n'))
}

// ExecutorConfig holds configuration for the Safe Tool Execution Engine.
type ExecutorConfig struct {
	Registry    *tool.Registry
	Policy      *PolicyEngine
	AuditLogger AuditLogger
	Timeout     time.Duration // Default max timeout, e.g. 3 seconds
}

// Executor wraps tool invocation with schema validation, RBAC, timeouts, and audit logging.
type Executor struct {
	registry    *tool.Registry
	policy      *PolicyEngine
	auditLogger AuditLogger
	timeout     time.Duration
}

// NewExecutor creates a new safe tool Executor.
func NewExecutor(cfg ExecutorConfig) *Executor {
	if cfg.Registry == nil {
		cfg.Registry = tool.NewRegistry()
	}
	if cfg.Policy == nil {
		cfg.Policy = NewPolicyEngine()
	}
	if cfg.AuditLogger == nil {
		cfg.AuditLogger = NewJSONAuditLogger(os.Stdout)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 3 * time.Second
	}

	return &Executor{
		registry:    cfg.Registry,
		policy:      cfg.Policy,
		auditLogger: cfg.AuditLogger,
		timeout:     cfg.Timeout,
	}
}

// Registry returns the underlying tool registry.
func (e *Executor) Registry() *tool.Registry {
	return e.registry
}

// Execute safely runs a tool by name with strict schema validation, authorization, and timeouts.
func (e *Executor) Execute(ctx context.Context, toolName string, args map[string]any) (any, error) {
	start := time.Now()
	execID := newExecutionID()

	caller, ok := CallerFromContext(ctx)
	callerID := "anonymous"
	if ok && caller.ID != "" {
		callerID = caller.ID
	}

	audit := AuditLog{
		ExecutionID: execID,
		Timestamp:   start,
		CallerID:    callerID,
		ToolName:    toolName,
		Arguments:   args,
	}

	defer func() {
		audit.DurationMS = float64(time.Since(start).Microseconds()) / 1000.0
		e.auditLogger.Log(audit)
	}()

	// 1. Look up tool
	t, exists := e.registry.Get(toolName)
	if !exists {
		err := fmt.Errorf("%w: %s", tool.ErrToolNotFound, toolName)
		audit.Status = "failed"
		audit.Error = err.Error()
		return nil, err
	}

	// 2. Authorize via Policy Engine (RBAC, Read-Only, SQL check)
	if err := e.policy.Authorize(caller, t.Definition, args); err != nil {
		audit.Status = "unauthorized"
		audit.Error = err.Error()
		return nil, err
	}

	// 3. Schema Validation
	if err := tool.Validate(t.Definition.Parameters, args); err != nil {
		audit.Status = "schema_error"
		audit.Error = err.Error()
		return nil, err
	}

	// 4. Execute with strict Timeout & Panic Recovery
	timeoutCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	type execResult struct {
		output any
		err    error
	}

	resChan := make(chan execResult, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				resChan <- execResult{
					err: fmt.Errorf("%w: %v", ErrToolPanicked, r),
				}
			}
		}()

		out, err := t.Handler(timeoutCtx, args)
		resChan <- execResult{output: out, err: err}
	}()

	select {
	case <-timeoutCtx.Done():
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			audit.Status = "timeout"
			audit.Error = ErrToolTimeout.Error()
			return nil, ErrToolTimeout
		}
		audit.Status = "canceled"
		audit.Error = timeoutCtx.Err().Error()
		return nil, timeoutCtx.Err()

	case res := <-resChan:
		if res.err != nil {
			audit.Status = "failed"
			audit.Error = res.err.Error()
			return nil, res.err
		}

		audit.Status = "success"
		audit.Output = res.output
		return res.output, nil
	}
}

func newExecutionID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "exec-" + hex.EncodeToString(b)
}
