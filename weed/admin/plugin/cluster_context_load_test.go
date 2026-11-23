package plugin

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// A provider stuck on master discovery must not hold a preview past its deadline.
func TestLoadClusterContextReturnsAtCallerDeadline(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	r := &Plugin{clusterContextProvider: func(context.Context) (*plugin_pb.ClusterContext, error) {
		<-release
		return &plugin_pb.ClusterContext{}, nil
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := r.loadSchedulerClusterContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("returned after %v, want about the caller's 100ms deadline", elapsed)
	}
}
