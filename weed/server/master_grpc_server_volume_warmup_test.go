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

// A leader that already knew volumes at the leader change is warming up until
// every volume server has had time to report again.
func newWarmingUpMaster(t *testing.T) *MasterServer {
	t.Helper()
	ms := newLeaderMaster()
	node := ms.Topo.GetOrCreateDataCenter("dc1").GetOrCreateRack("rack1").
		GetOrCreateDataNode("127.0.0.1", 8080, 18080, "127.0.0.1", "node1", map[string]uint32{"": 10})
	ms.Topo.SyncDataNodeRegistration([]*master_pb.VolumeInformationMessage{{Id: 7, Size: 100}}, node)
	ms.Topo.SetLastLeaderChangeTime(time.Now())
	if !ms.Topo.IsWarmingUp() {
		t.Fatal("precondition: master is not warming up")
	}
	return ms
}

func TestLookupVolumeDuringWarmupRefusesPartialNotFound(t *testing.T) {
	ms := newWarmingUpMaster(t)
	_, err := ms.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{VolumeOrFileIds: []string{"7", "8"}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("partial not-found during warmup = %v, want Unavailable", err)
	}
}

func TestLookupVolumeDuringWarmupAnswersFullyKnownBatch(t *testing.T) {
	ms := newWarmingUpMaster(t)
	resp, err := ms.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{VolumeOrFileIds: []string{"7"}})
	if err != nil {
		t.Fatalf("known volume during warmup: %v", err)
	}
	if len(resp.VolumeIdLocations) != 1 || len(resp.VolumeIdLocations[0].Locations) != 1 || resp.VolumeIdLocations[0].Error != "" {
		t.Fatalf("known volume answer = %+v", resp.VolumeIdLocations)
	}
}

func TestLookupVolumeAfterWarmupReportsNotFoundPerVolume(t *testing.T) {
	ms := newWarmingUpMaster(t)
	ms.Topo.SetLastLeaderChangeTime(time.Now().Add(-time.Hour))
	resp, err := ms.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{VolumeOrFileIds: []string{"7", "8"}})
	if err != nil {
		t.Fatalf("lookup after warmup: %v", err)
	}
	if len(resp.VolumeIdLocations) != 2 {
		t.Fatalf("answers = %+v, want two", resp.VolumeIdLocations)
	}
	if len(resp.VolumeIdLocations[0].Locations) != 1 || resp.VolumeIdLocations[0].Error != "" {
		t.Fatalf("known volume answer = %+v", resp.VolumeIdLocations[0])
	}
	if len(resp.VolumeIdLocations[1].Locations) != 0 || resp.VolumeIdLocations[1].Error == "" {
		t.Fatalf("absent volume answer = %+v", resp.VolumeIdLocations[1])
	}
}

// A LookupVolume refused during warm-up carries the same hint, so the
// masterclient's lookup retry (RetryWithBackoff) also lands at warm-up end.
func TestLookupVolumeDuringWarmupCarriesRemainingWarmup(t *testing.T) {
	ms := newWarmingUpMaster(t)
	want := ms.Topo.RemainingWarmupDuration()
	_, err := ms.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{VolumeOrFileIds: []string{"7", "8"}})
	st, _ := status.FromError(err)
	var hint *errdetails.RetryInfo
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			hint = ri
		}
	}
	if st.Code() != codes.Unavailable || hint == nil {
		t.Fatalf("LookupVolume during warm-up = %v with details %v, want Unavailable with RetryInfo", err, st.Details())
	}
	if got := hint.GetRetryDelay().AsDuration(); got <= 0 || got > want || want-got > time.Second {
		t.Fatalf("RetryInfo delay = %v, want the remaining warm-up (about %v)", got, want)
	}
}
