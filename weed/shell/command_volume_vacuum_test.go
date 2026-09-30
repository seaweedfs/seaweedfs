package shell

import (
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

func vacuumTestTopology(nodes ...*master_pb.DataNodeInfo) *master_pb.TopologyInfo {
	return &master_pb.TopologyInfo{
		DataCenterInfos: []*master_pb.DataCenterInfo{{
			RackInfos: []*master_pb.RackInfo{{DataNodeInfos: nodes}},
		}},
	}
}

func vacuumTestNode(volumes ...*master_pb.VolumeInformationMessage) *master_pb.DataNodeInfo {
	return &master_pb.DataNodeInfo{
		DiskInfos: map[string]*master_pb.DiskInfo{"": {VolumeInfos: volumes}},
	}
}

func TestReadOnlyVolumesAboveThreshold(t *testing.T) {
	readOnlyHalfGarbage := &master_pb.VolumeInformationMessage{Id: 1, Collection: "a", Size: 100, DeletedByteCount: 50, ReadOnly: true}
	writableMostlyGarbage := &master_pb.VolumeInformationMessage{Id: 2, Collection: "a", Size: 100, DeletedByteCount: 90}
	readOnlyLittleGarbage := &master_pb.VolumeInformationMessage{Id: 3, Collection: "a", Size: 100, DeletedByteCount: 10, ReadOnly: true}
	readOnlyOtherCollection := &master_pb.VolumeInformationMessage{Id: 4, Collection: "b", Size: 100, DeletedByteCount: 100, ReadOnly: true}
	readOnlyEmpty := &master_pb.VolumeInformationMessage{Id: 5, Collection: "a", Size: 0, DeletedByteCount: 0, ReadOnly: true}
	readOnlyAtThreshold := &master_pb.VolumeInformationMessage{Id: 6, Collection: "a", Size: 100, DeletedByteCount: 30, ReadOnly: true}
	// the sweep skips a volume when any replica is read-only, and the garbage
	// may sit on the other replica
	mixedReadOnlyReplica := &master_pb.VolumeInformationMessage{Id: 7, Collection: "a", Size: 100, DeletedByteCount: 5, ReadOnly: true}
	mixedWritableReplica := &master_pb.VolumeInformationMessage{Id: 7, Collection: "a", Size: 100, DeletedByteCount: 60}
	// a converted index reports deletes without their sizes
	readOnlyConvertedIndex := &master_pb.VolumeInformationMessage{Id: 8, Collection: "a", Size: 100, DeleteCount: 2, DeletedByteCount: 0, ReadOnly: true}

	topo := vacuumTestTopology(
		vacuumTestNode(readOnlyHalfGarbage, writableMostlyGarbage, readOnlyLittleGarbage, readOnlyEmpty, readOnlyAtThreshold, mixedReadOnlyReplica, readOnlyConvertedIndex),
		// the second replica of volume 1 must not list it twice
		vacuumTestNode(readOnlyHalfGarbage, readOnlyOtherCollection, mixedWritableReplica),
	)

	got := readOnlyVolumesAboveThreshold(topo, "a", 0.3)
	if want := []uint32{1, 6, 7, 8}; !reflect.DeepEqual(got, want) {
		t.Fatalf("collection a, threshold 0.3: got %v, want %v", got, want)
	}

	got = readOnlyVolumesAboveThreshold(topo, "", 0.3)
	if want := []uint32{1, 4, 6, 7, 8}; !reflect.DeepEqual(got, want) {
		t.Fatalf("all collections, threshold 0.3: got %v, want %v", got, want)
	}

	if got := readOnlyVolumesAboveThreshold(topo, "a", 0.95); !reflect.DeepEqual(got, []uint32{8}) {
		t.Fatalf("threshold 0.95: got %v, want only the converted index [8]", got)
	}
}
