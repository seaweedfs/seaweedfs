package engine

import (
	"errors"
	"fmt"
	"testing"
)

func TestNoSchemaError(t *testing.T) {
	// Test creating a NoSchemaError
	err := NoSchemaError{Namespace: "test", Topic: "topic1"}
	expectedMsg := "topic test.topic1 has no schema"
	if err.Error() != expectedMsg {
		t.Errorf("Expected error message '%s', got '%s'", expectedMsg, err.Error())
	}

	// Test IsNoSchemaError with direct NoSchemaError
	if !IsNoSchemaError(err) {
		t.Error("IsNoSchemaError should return true for NoSchemaError")
	}

	// Test IsNoSchemaError with wrapped NoSchemaError
	wrappedErr := fmt.Errorf("wrapper: %w", err)
	if !IsNoSchemaError(wrappedErr) {
		t.Error("IsNoSchemaError should return true for wrapped NoSchemaError")
	}

	// Test IsNoSchemaError with different error type
	otherErr := errors.New("different error")
	if IsNoSchemaError(otherErr) {
		t.Error("IsNoSchemaError should return false for other error types")
	}

	// Test IsNoSchemaError with nil
	if IsNoSchemaError(nil) {
		t.Error("IsNoSchemaError should return false for nil")
	}
}
