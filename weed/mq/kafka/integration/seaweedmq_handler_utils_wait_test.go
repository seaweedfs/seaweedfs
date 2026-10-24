package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
)

// A fresh filer is listed by the masters only after its announce delay, so an
// empty list must be waited out, not handed to the filer client accessor
// (which panics on it).
func TestWaitForFilersWaitsOutEmptyListing(t *testing.T) {
	calls := 0
	filers, err := waitForClusterNodes(context.Background(), "filers", 10*time.Second, func() ([]pb.ServerAddress, error) {
		calls++
		if calls < 3 {
			return nil, nil
		}
		return []pb.ServerAddress{"filer:8888"}, nil
	})
	if err != nil || len(filers) != 1 || calls != 3 {
		t.Fatalf("filers %v err %v after %d calls, want 1 filer after 3", filers, err, calls)
	}
}

func TestWaitForFilersGivesUpWithAnError(t *testing.T) {
	filers, err := waitForClusterNodes(context.Background(), "filers", 1500*time.Millisecond, func() ([]pb.ServerAddress, error) { return nil, nil })
	if err == nil || len(filers) != 0 {
		t.Fatalf("still empty: filers %v err %v, want an error", filers, err)
	}
	boom := errors.New("master down")
	if _, err := waitForClusterNodes(context.Background(), "filers", time.Minute, func() ([]pb.ServerAddress, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("discovery error: got %v, want it returned at once", err)
	}
}
