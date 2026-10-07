package task

import (
	"context"
	"errors"
	"testing"
)

func TestValidateTransition(t *testing.T) {
	tests := []struct {
		name      string
		from      State
		to        State
		expectErr bool
	}{
		// Valid transitions
		{"PENDING to RUNNING", StatePending, StateRunning, false},
		{"RUNNING to COMPLETED", StateRunning, StateCompleted, false},
		{"RUNNING to FAILED", StateRunning, StateFailed, false},
		{"RUNNING to DLQ", StateRunning, StateDLQ, false},
		{"FAILED to PENDING (retry)", StateFailed, StatePending, false},
		{"FAILED to DLQ (exhausted)", StateFailed, StateDLQ, false},

		// Invalid transitions
		{"COMPLETED to RUNNING (terminal state)", StateCompleted, StateRunning, true},
		{"COMPLETED to PENDING", StateCompleted, StatePending, true},
		{"COMPLETED to DLQ", StateCompleted, StateDLQ, true},
		{"DLQ to RUNNING (terminal state)", StateDLQ, StateRunning, true},
		{"DLQ to PENDING", StateDLQ, StatePending, true},
		{"PENDING to COMPLETED (must run first)", StatePending, StateCompleted, true},
		{"PENDING to DLQ (must run first)", StatePending, StateDLQ, true},
		{"PENDING to FAILED (must run first)", StatePending, StateFailed, true},
		{"FAILED to COMPLETED (must retry and run)", StateFailed, StateCompleted, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTransition(tc.from, tc.to)
			if tc.expectErr && err == nil {
				t.Fatalf("expected error transitioning from %s to %s, got nil", tc.from, tc.to)
			}
			if !tc.expectErr && err != nil {
				t.Fatalf("did not expect error transitioning from %s to %s, got: %v", tc.from, tc.to, err)
			}
			if tc.expectErr && !errors.Is(err, ErrInvalidStateTransition) {
				t.Fatalf("expected ErrInvalidStateTransition, got: %v", err)
			}
		})
	}
}

func TestJob_TransitionTo(t *testing.T) {
	job := NewJob("job-123", "email_notification", []byte(`{"to":"test@example.com"}`), 3)

	if job.State() != StatePending {
		t.Fatalf("initial state should be PENDING, got %s", job.State())
	}

	// Valid transition: PENDING -> RUNNING
	if err := job.TransitionTo(StateRunning); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if job.State() != StateRunning {
		t.Fatalf("state should be RUNNING, got %s", job.State())
	}

	// Invalid transition: RUNNING -> PENDING
	if err := job.TransitionTo(StatePending); err == nil {
		t.Fatalf("expected error for RUNNING -> PENDING transition")
	}

	// Valid transition: RUNNING -> COMPLETED
	if err := job.TransitionTo(StateCompleted); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Invalid transition: COMPLETED -> RUNNING
	if err := job.TransitionTo(StateRunning); err == nil {
		t.Fatalf("expected error for COMPLETED -> RUNNING transition")
	}
}

func TestJob_RetryLogic(t *testing.T) {
	job := NewJob("job-retry", "task", nil, 2)

	if !job.CanRetry() {
		t.Fatalf("job should be able to retry initially")
	}

	job.IncrementRetry()
	if job.RetryCount != 1 || !job.CanRetry() {
		t.Fatalf("job should be able to retry at count 1")
	}

	job.IncrementRetry()
	if job.RetryCount != 2 || job.CanRetry() {
		t.Fatalf("job should not be able to retry when RetryCount reaches MaxRetries")
	}
}

func TestCustomErrors(t *testing.T) {
	baseErr := errors.New("connection reset by peer")
	tempErr := NewTemporaryError(baseErr)

	if !IsTemporary(tempErr) {
		t.Fatalf("expected IsTemporary to return true")
	}
	if IsFatal(tempErr) {
		t.Fatalf("expected IsFatal to return false for TemporaryError")
	}
	if !errors.Is(tempErr, baseErr) {
		t.Fatalf("expected errors.Is to match wrapped base error")
	}

	fatalBase := errors.New("invalid json payload")
	fatalErr := NewFatalError(fatalBase)

	if !IsFatal(fatalErr) {
		t.Fatalf("expected IsFatal to return true")
	}
	if IsTemporary(fatalErr) {
		t.Fatalf("expected IsTemporary to return false for FatalError")
	}
	if !errors.Is(fatalErr, fatalBase) {
		t.Fatalf("expected errors.Is to match wrapped fatal error")
	}
}

func TestHandlerFunc(t *testing.T) {
	var called bool
	handler := HandlerFunc(func(ctx context.Context, job Job) error {
		called = true
		return nil
	})

	err := handler.Process(context.Background(), *NewJob("1", "test", nil, 1))
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if !called {
		t.Fatalf("handler was not called")
	}
}
