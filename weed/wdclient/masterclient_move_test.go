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

// The change listener hears only the changes updateVidMap applied: a volume
// moved between the server's disks is reported both ways and stays put, so
// it arrives as an add, never a removal.
func TestChangeListenerHearsAppliedChanges(t *testing.T) {
	mc := moveClient()
	var heardAdds, heardDels []uint32
	mc.AddOnVolumeLocationChangeFn(func(_ Location, addedVids []uint32, deletedVids []uint32, _ []uint32, _ []uint32) {
		heardAdds = append(heardAdds, addedVids...)
		heardDels = append(heardDels, deletedVids...)
	})

	mc.updateVidMap(moveResponse([]uint32{1}, []uint32{1, 2}))

	if len(heardAdds) != 1 || heardAdds[0] != 1 {
		t.Errorf("listener heard adds %v, want [1]", heardAdds)
	}
	if len(heardDels) != 1 || heardDels[0] != 2 {
		t.Errorf("listener heard removals %v, want [2]", heardDels)
	}
}

// Several caches may share one MasterClient: every registered listener hears
// the applied changes, and unregistering stops the delivery.
func TestMultipleListenersHearAndUnregister(t *testing.T) {
	mc := moveClient()
	var heard1, heard2 int
	mc.AddOnVolumeLocationChangeFn(func(_ Location, _ []uint32, _ []uint32, _ []uint32, _ []uint32) {
		heard1++
	})
	unhook := mc.AddOnVolumeLocationChangeFn(func(_ Location, _ []uint32, _ []uint32, _ []uint32, _ []uint32) {
		heard2++
	})

	mc.updateVidMap(moveResponse(nil, []uint32{1}))
	if heard1 != 1 || heard2 != 1 {
		t.Fatalf("listeners heard %d, %d, want 1, 1", heard1, heard2)
	}

	unhook()
	mc.updateVidMap(moveResponse(nil, []uint32{2}))
	if heard1 != 2 || heard2 != 1 {
		t.Errorf("after unhook listeners heard %d, %d, want 2, 1", heard1, heard2)
	}
}
