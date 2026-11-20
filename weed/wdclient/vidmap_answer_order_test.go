package wdclient

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// An answer to a lookup asked before a volume was learned again must not
// replace what that learning set aside, also when the answer adds locations.
// Here an overlapping lookup's answer teaches the cache the new location
// without moving the epoch, a read invalidates it, and a failed refresh
// revives it. The older answer, which has the old location, arrives after.
func TestOlderAnswerDoesNotReplaceNewerRevivedLocations(t *testing.T) {
	provider := &stubProvider{}
	vc := newVidMapClient(provider, "", DefaultVidMapCacheSize)
	provider.answer = func(call int) (map[string][]Location, error) {
		if call > 1 {
			return nil, errNoAnswer
		}
		vc.addLocation(5, Location{Url: "new:8080"})
		vc.InvalidateCache("5,0102")
		if _, err := vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5"}); err != nil {
			t.Errorf("overlapping lookup: %v", err)
		}
		return map[string][]Location{"5": {{Url: "old:8080"}}, "6": {{Url: "six:8080"}}}, nil
	}

	vc.LookupVolumeIdsWithFallback(context.Background(), []string{"5", "6"})

	requireLocations(t, vc, 5, "new:8080")
	requireLocations(t, vc, 6, "six:8080")
}

// answeringMaster answers every volume at the given servers. When
// pushDuringLookup is set, the master's stream first reports volume 5 on a new
// server, a read fails there and a refresh that got no answer brings it back,
// all while this answer is on its way.
type answeringMaster struct {
	master_pb.UnimplementedSeaweedServer
	mc               *MasterClient
	answer           []string
	pushDuringLookup bool
}

func (s *answeringMaster) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	if s.pushDuringLookup {
		pushVolumeLocation(s.mc, "new:8080", []uint32{5}, nil, nil, nil)
		s.mc.InvalidateCache("5,0102")
		s.mc.vidMap.reviveStale(5) // what the refresh that got no answer does
	}
	resp := &master_pb.LookupVolumeResponse{}
	for _, vid := range req.VolumeOrFileIds {
		var locations []*master_pb.Location
		for _, url := range s.answer {
			locations = append(locations, &master_pb.Location{Url: url, PublicUrl: url})
		}
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeOrFileId: vid,
			Locations:      locations,
		})
	}
	return resp, nil
}

// The master client's provider caches what it found itself, before the
// lookup that called it checks what was set aside, so the order must hold
// there too. A pushed location does not move the epoch: only learnedAt tells
// that the answer is older. An answer with nothing newer in the way still
// replaces what was set aside, with every replica it reports.
func TestMasterAnswerAgainstSetAsideLocations(t *testing.T) {
	for _, tc := range []struct {
		name             string
		pushDuringLookup bool
		answer           []string
		want             []string
	}{
		{"newer location revived during the lookup", true, []string{"old:8080"}, []string{"new:8080"}},
		{"nothing newer", false, []string{"a:8080", "b:8080"}, []string{"a:8080", "b:8080"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mc := NewMasterClient(
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				"", "test", "", "", "",
				pb.ServerDiscovery{},
			)
			srv := &answeringMaster{mc: mc, answer: tc.answer, pushDuringLookup: tc.pushDuringLookup}
			mc.setCurrentMaster(startFakeMasterServer(t, srv))
			mc.grpcTimeout = 5 * time.Second
			pushVolumeLocation(mc, "gone:8080", []uint32{5}, nil, nil, nil)
			mc.InvalidateCache("5,0102")

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := mc.LookupVolumeIdsWithFallback(ctx, []string{"5"}); err != nil {
				t.Fatalf("lookup: %v", err)
			}

			requireLocations(t, mc.vidMapClient, 5, tc.want...)
		})
	}
}
