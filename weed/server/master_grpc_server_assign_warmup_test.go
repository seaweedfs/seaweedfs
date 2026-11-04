package weed_server

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// assertWarmupHint calls refuse during warm-up and checks it is refused with
// Unavailable and a RetryInfo delay within the remaining warm-up measured
// around the call.
func assertWarmupHint(t *testing.T, ms *MasterServer, refuse func() error) {
	t.Helper()
	before := ms.Topo.RemainingWarmupDuration()
	err := refuse()
	after := ms.Topo.RemainingWarmupDuration()
	st, _ := status.FromError(err)
	var hint *errdetails.RetryInfo
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			hint = ri
		}
	}
	if st.Code() != codes.Unavailable || hint == nil {
		t.Fatalf("refused with %v and details %v, want Unavailable with RetryInfo", err, st.Details())
	}
	if got := hint.GetRetryDelay().AsDuration(); got <= 0 || got > before || got < after {
		t.Fatalf("RetryInfo delay = %v, want within [%v, %v]", got, after, before)
	}
}

func TestAssignDuringWarmupCarriesRemainingWarmup(t *testing.T) {
	ms := newWarmingUpMaster(t)
	assertWarmupHint(t, ms, func() error {
		_, err := ms.Assign(context.Background(), &master_pb.AssignRequest{Count: 1})
		return err
	})
}

func TestWarmingUpErrorWithoutRemainingHasNoHint(t *testing.T) {
	st, _ := status.FromError(warmingUpError(0))
	if st.Code() != codes.Unavailable || len(st.Details()) != 0 {
		t.Fatalf("got %v with details %v, want plain Unavailable", st.Code(), st.Details())
	}
}
