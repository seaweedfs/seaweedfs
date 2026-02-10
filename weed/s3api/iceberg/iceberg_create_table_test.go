package iceberg

import (
	"errors"
	"testing"
)

func TestValidateCreateTableRequestRejectsStageCreate(t *testing.T) {
	err := validateCreateTableRequest(CreateTableRequest{Name: "orders", StageCreate: true})
	if !errors.Is(err, errStageCreateUnsupported) {
		t.Fatalf("validateCreateTableRequest() error = %v, want errStageCreateUnsupported", err)
	}
}

func TestValidateCreateTableRequestRequiresName(t *testing.T) {
	err := validateCreateTableRequest(CreateTableRequest{})
	if err == nil {
		t.Fatalf("validateCreateTableRequest() expected error")
	}
}

func TestValidateCreateTableRequestAcceptsStandardCreate(t *testing.T) {
	err := validateCreateTableRequest(CreateTableRequest{Name: "orders"})
	if err != nil {
		t.Fatalf("validateCreateTableRequest() error = %v, want nil", err)
	}
}
