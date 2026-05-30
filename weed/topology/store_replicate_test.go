package topology

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/operation"
)

// TestDistributedOperationCancelsSiblingsOnFirstError verifies that once one
// replica fails, an outstanding replica still stalled in a dial timeout is
// cancelled rather than gating the caller until it times out.
func TestDistributedOperationCancelsSiblingsOnFirstError(t *testing.T) {
	locations := []operation.Location{{Url: "fast"}, {Url: "slow"}}
	cancelled := make(chan struct{}, 1)

	start := time.Now()
	err := DistributedOperation(context.Background(), locations, func(ctx context.Context, location operation.Location) error {
		if location.Url == "fast" {
			return errors.New("connection refused")
		}
		// slow: a replica stalled in a dial timeout
		select {
		case <-ctx.Done():
			cancelled <- struct{}{}
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	})

	if err == nil {
		t.Fatal("expected an error from the fast-failing replica")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("did not fail fast: took %v", elapsed)
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("slow replica was not cancelled after the first error")
	}
}

func TestDistributedOperationEmpty(t *testing.T) {
	err := DistributedOperation(context.Background(), nil, func(ctx context.Context, location operation.Location) error {
		t.Fatal("op should not be called when there are no locations")
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil for no locations, got %v", err)
	}
}
