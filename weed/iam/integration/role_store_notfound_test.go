package integration

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryRoleStoreWrapsErrRoleNotFound(t *testing.T) {
	_, err := NewMemoryRoleStore().GetRole(context.Background(), "", "missing")
	if !errors.Is(err, ErrRoleNotFound) {
		t.Fatalf("missing role error does not wrap ErrRoleNotFound: %v", err)
	}
}
