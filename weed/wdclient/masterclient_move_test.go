package wdclient

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func moveClient() *MasterClient {
	return &MasterClient{vidMapClient: newVidMapClient(nil, "", 0)}
}

func moveResponse(newVids, deletedVids []uint32) *master_pb.KeepConnectedResponse {
	return &master_pb.KeepConnectedResponse{VolumeLocation: &master_pb.VolumeLocation{
		Url: "server:8080", PublicUrl: "server:8080", NewVids: newVids, DeletedVids: deletedVids,
	}}
}

// A volume moved between a server's disks arrives added and removed at once,
// and the server still has it.
func TestMovedVolumeKeepsItsLocation(t *testing.T) {
	mc := moveClient()
	mc.updateVidMap(moveResponse([]uint32{1}, nil))
	mc.updateVidMap(moveResponse([]uint32{1}, []uint32{1}))

	locations, found := mc.GetLocations(1)
	if !found || len(locations) != 1 {
		t.Errorf("a volume that moved between its server's disks lost its location: found=%v %v", found, locations)
	}
}

func TestRemovedVolumeLosesItsLocation(t *testing.T) {
	mc := moveClient()
	mc.updateVidMap(moveResponse([]uint32{1}, nil))
	mc.updateVidMap(moveResponse(nil, []uint32{1}))

	if locations, found := mc.GetLocations(1); found && len(locations) > 0 {
		t.Errorf("a volume that left the server kept its location: %v", locations)
	}
}

func TestReadOnlyVolumeUpdatesItsLocation(t *testing.T) {
	mc := moveClient()
	mc.updateVidMap(moveResponse([]uint32{1}, nil))
	mc.updateVidMap(&master_pb.KeepConnectedResponse{VolumeLocation: &master_pb.VolumeLocation{
		Url: "server:8080", PublicUrl: "server:8080", NewVids: []uint32{1}, ReadOnlyVids: []uint32{1},
	}})

	locations, found := mc.GetLocations(1)
	if !found || len(locations) != 1 || !locations[0].ReadOnly {
		t.Fatalf("read-only update = %v, want one read-only location", locations)
	}
}

// Removals of other volumes in the same message must still apply.
func TestRemovalsAlongsideAMoveStillApply(t *testing.T) {
	mc := moveClient()
	mc.updateVidMap(moveResponse([]uint32{1, 2}, nil))
	mc.updateVidMap(moveResponse([]uint32{1}, []uint32{1, 2}))

	if locations, found := mc.GetLocations(1); !found || len(locations) != 1 {
		t.Errorf("the moved volume lost its location: found=%v %v", found, locations)
	}
	if locations, found := mc.GetLocations(2); found && len(locations) > 0 {
		t.Errorf("a volume that left the server kept its location: %v", locations)
	}
}

func moveEcResponse(newEcVids, deletedEcVids []uint32) *master_pb.KeepConnectedResponse {
	return &master_pb.KeepConnectedResponse{VolumeLocation: &master_pb.VolumeLocation{
		Url: "server:8080", PublicUrl: "server:8080", NewEcVids: newEcVids, DeletedEcVids: deletedEcVids,
	}}
}

func TestEcVolumeMovedKeepsItsLocation(t *testing.T) {
	mc := moveClient()
	mc.updateVidMap(moveEcResponse([]uint32{7}, nil))
	mc.updateVidMap(moveEcResponse([]uint32{7}, []uint32{7}))

	if locations, found := mc.GetLocations(7); !found || len(locations) != 1 {
		t.Errorf("an ec volume reported both ways lost its location: found=%v %v", found, locations)
	}
}

func TestEcVolumeRemovedLosesItsLocation(t *testing.T) {
	mc := moveClient()
	mc.updateVidMap(moveEcResponse([]uint32{7}, nil))
	mc.updateVidMap(moveEcResponse(nil, []uint32{7}))

	if locations, found := mc.GetLocations(7); found && len(locations) > 0 {
		t.Errorf("an ec volume that left the server kept its location: %v", locations)
	}
}

// The delete listener hears only the removals updateVidMap applied: a volume
// moved between the server's disks is reported both ways and stays put.
func TestDeleteListenerHearsOnlyRealRemovals(t *testing.T) {
	mc := moveClient()
	var heard []uint32
	mc.SetOnVolumeLocationDeleteFn(func(_ Location, deletedVids []uint32, _ []uint32) {
		heard = append(heard, deletedVids...)
	})

	mc.updateVidMap(moveResponse([]uint32{1}, []uint32{1, 2}))

	if len(heard) != 1 || heard[0] != 2 {
		t.Errorf("listener heard %v, want [2]", heard)
	}
}
