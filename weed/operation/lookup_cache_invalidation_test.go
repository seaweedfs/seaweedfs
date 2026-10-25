package operation

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeLookupServer struct {
	master_pb.UnimplementedSeaweedServer
	mu        sync.Mutex
	calls     int
	locations int
}

func (s *fakeLookupServer) LookupVolume(_ context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	resp := &master_pb.LookupVolumeResponse{}
	for _, vid := range req.VolumeOrFileIds {
		var locs []*master_pb.Location
		for i := 0; i < s.locations; i++ {
			locs = append(locs, &master_pb.Location{Url: fmt.Sprintf("10.0.0.%d:8080", i)})
		}
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: vid,
			Locations:      locs,
		})
	}
	return resp, nil
}

// TestLookupVolumeIdCacheInvalidation reproduces the stale-lookup scenario: a
// volume that briefly reports too few replicas would be cached for the full TTL,
// so writes kept failing even after the master re-registered the missing replica.
// InvalidateVolumeIdLocationCache forces the next lookup to re-query the master.
func TestLookupVolumeIdCacheInvalidation(t *testing.T) {
	fake := &fakeLookupServer{locations: 1}
	master := startFakeMasterServer(t, fake)
	masterFn := func(context.Context) pb.ServerAddress { return master }
	dial := grpc.WithTransportCredentials(insecure.NewCredentials())

	const vid = "778899"
	InvalidateVolumeIdLocationCache(vid)

	// first lookup queries the master and caches the single, under-replicated location
	r1, err := LookupVolumeId(masterFn, dial, vid)
	if err != nil {
		t.Fatalf("first lookup: %v", err)
	}
	if len(r1.Locations) != 1 {
		t.Fatalf("first lookup locations = %d, want 1", len(r1.Locations))
	}

	// a second lookup is served from cache, so the master is not queried again
	if _, err := LookupVolumeId(masterFn, dial, vid); err != nil {
		t.Fatalf("second lookup: %v", err)
	}
	fake.mu.Lock()
	calls := fake.calls
	fake.locations = 2 // master heals: both replicas are registered again
	fake.mu.Unlock()
	if calls != 1 {
		t.Fatalf("expected a cache hit, master queried %d times", calls)
	}

	// invalidation drops the stale entry, so the next lookup re-queries and sees both replicas
	InvalidateVolumeIdLocationCache(vid)
	r3, err := LookupVolumeId(masterFn, dial, vid)
	if err != nil {
		t.Fatalf("third lookup: %v", err)
	}
	if len(r3.Locations) != 2 {
		t.Fatalf("after invalidation locations = %d, want 2", len(r3.Locations))
	}
}
