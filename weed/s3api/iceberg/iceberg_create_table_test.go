package iceberg

import (
	"errors"
	"testing"
)

func TestValidateCreateTableRequestRequiresName(t *testing.T) {
	err := validateCreateTableRequest(CreateTableRequest{})
	if !errors.Is(err, errTableNameRequired) {
		t.Fatalf("validateCreateTableRequest() error = %v, want errTableNameRequired", err)
	}
}

func TestValidateCreateTableRequestAcceptsWithName(t *testing.T) {
	err := validateCreateTableRequest(CreateTableRequest{Name: "orders"})
	if err != nil {
		t.Fatalf("validateCreateTableRequest() error = %v, want nil", err)
	}
}

func TestIsStageCreateEnabledDefaultsToTrue(t *testing.T) {
	t.Setenv("ICEBERG_ENABLE_STAGE_CREATE", "")
	if !isStageCreateEnabled() {
		t.Fatalf("isStageCreateEnabled() = false, want true")
	}
}

func TestIsStageCreateEnabledFalseValues(t *testing.T) {
	falseValues := []string{"0", "false", "FALSE", "no", "off"}
	for _, value := range falseValues {
		t.Setenv("ICEBERG_ENABLE_STAGE_CREATE", value)
		if isStageCreateEnabled() {
			t.Fatalf("isStageCreateEnabled() = true for value %q, want false", value)
		}
	}
}
