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

func listenForChanges(mc *MasterClient) (adds, dels *[]uint32) {
	adds, dels = new([]uint32), new([]uint32)
	mc.SetOnVolumeLocationChangeFn(func(_ Location, addedVids, deletedVids []uint32) {
		*adds = append(*adds, addedVids...)
		*dels = append(*dels, deletedVids...)
	})
	return adds, dels
}

func pushVolumeLocation(mc *MasterClient, url string, newVids, deletedVids, newEcVids, deletedEcVids []uint32) {
	mc.updateVidMap(&master_pb.KeepConnectedResponse{VolumeLocation: &master_pb.VolumeLocation{
		Url: url, PublicUrl: url, NewVids: newVids, DeletedVids: deletedVids, NewEcVids: newEcVids, DeletedEcVids: deletedEcVids,
	}})
}

// A volume moved between the server's disks arrives as an add, never a removal.
func TestChangeListenerHearsAppliedChanges(t *testing.T) {
	mc := moveClient()
	adds, dels := listenForChanges(mc)

	mc.updateVidMap(moveResponse([]uint32{1}, []uint32{1, 2}))

	if len(*adds) != 1 || (*adds)[0] != 1 {
		t.Errorf("listener heard adds %v, want [1]", *adds)
	}
	if len(*dels) != 1 || (*dels)[0] != 2 {
		t.Errorf("listener heard removals %v, want [2]", *dels)
	}
}

func TestChangeListenerSkipsRemovalWhileOtherFormServes(t *testing.T) {
	mc := moveClient()
	_, dels := listenForChanges(mc)

	pushVolumeLocation(mc, "server:8080", []uint32{1}, nil, []uint32{1}, nil)
	pushVolumeLocation(mc, "server:8080", nil, nil, nil, []uint32{1})
	if len(*dels) != 0 {
		t.Fatalf("shard removal reported while the regular copy stays: %v", *dels)
	}
	pushVolumeLocation(mc, "server:8080", nil, []uint32{1}, nil, nil)
	if len(*dels) != 1 || (*dels)[0] != 1 {
		t.Errorf("last form removed but listener heard %v, want [1]", *dels)
	}
}

// The shards and the regular copy's removal can share one message, where
// the removal is applied before the shards are added.
func TestChangeListenerSkipsRemovalReplacedInSameMessage(t *testing.T) {
	mc := moveClient()
	pushVolumeLocation(mc, "server:8080", []uint32{1}, nil, nil, nil)
	_, dels := listenForChanges(mc)

	pushVolumeLocation(mc, "server:8080", nil, []uint32{1}, []uint32{1}, nil)

	if len(*dels) != 0 {
		t.Errorf("regular copy replaced by shards in one message reported removed: %v", *dels)
	}
}

// Shards only an earlier master generation reported do not keep a server
// alive once the regular copy goes.
func TestChangeListenerIgnoresStaleGenerationWhenSuppressing(t *testing.T) {
	mc := moveClient()
	pushVolumeLocation(mc, "server:8080", []uint32{1}, nil, []uint32{1}, nil)
	mc.resetVidMap()
	pushVolumeLocation(mc, "server:8080", []uint32{1}, nil, nil, nil)
	_, dels := listenForChanges(mc)

	pushVolumeLocation(mc, "server:8080", nil, []uint32{1}, nil, nil)

	if len(*dels) != 1 || (*dels)[0] != 1 {
		t.Errorf("listener heard removals %v, want [1]", *dels)
	}
}

func TestResetVidMapNotifiesListener(t *testing.T) {
	mc := moveClient()
	resets := 0
	mc.SetOnVidMapResetFn(func() { resets++ })

	mc.resetVidMap()

	if resets != 1 {
		t.Errorf("reset listener heard %d resets, want 1", resets)
	}
}
