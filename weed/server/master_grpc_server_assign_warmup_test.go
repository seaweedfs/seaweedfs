package weed_server

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// During warm-up, Assign is refused with Unavailable and a RetryInfo detail
// carrying the remaining warm-up, the gRPC twin of the HTTP Retry-After header.
func TestAssignDuringWarmupCarriesRemainingWarmup(t *testing.T) {
	ms := newWarmingUpMaster(t)
	want := ms.Topo.RemainingWarmupDuration()
	_, err := ms.Assign(context.Background(), &master_pb.AssignRequest{Count: 1})
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable {
		t.Fatalf("Assign during warm-up = %v, want Unavailable", err)
	}
	var hint *errdetails.RetryInfo
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			hint = ri
		}
	}
	if hint == nil {
		t.Fatalf("Assign during warm-up carries no RetryInfo: details %v", st.Details())
	}
	got := hint.GetRetryDelay().AsDuration()
	if got <= 0 || got > want || want-got > time.Second {
		t.Fatalf("RetryInfo delay = %v, want the remaining warm-up (about %v)", got, want)
	}
}

func TestWarmingUpAssignErrorWithoutRemainingHasNoHint(t *testing.T) {
	st, _ := status.FromError(warmingUpAssignError(0))
	if st.Code() != codes.Unavailable || len(st.Details()) != 0 {
		t.Fatalf("got %v with details %v, want plain Unavailable", st.Code(), st.Details())
	}
}
