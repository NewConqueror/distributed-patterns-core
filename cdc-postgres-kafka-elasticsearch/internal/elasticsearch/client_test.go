package elasticsearch

import (
	"errors"
	"net/http"
	"testing"
)

func TestIsTransient_Classification(t *testing.T) {
	transientErr1 := &TransientError{
		StatusCode: http.StatusServiceUnavailable,
		Message:    "503 Service Unavailable",
	}

	if !IsTransient(transientErr1) {
		t.Errorf("expected 503 error to be classified as transient")
	}

	transientErr2 := &TransientError{
		StatusCode: 0,
		Cause:      errors.New("dial tcp: connection refused"),
	}

	if !IsTransient(transientErr2) {
		t.Errorf("expected connection refused to be classified as transient")
	}

	permanentErr := errors.New("mapping error: invalid field type")
	if IsTransient(permanentErr) {
		t.Errorf("expected standard error not to be transient")
	}
}
