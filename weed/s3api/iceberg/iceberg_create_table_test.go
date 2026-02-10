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
