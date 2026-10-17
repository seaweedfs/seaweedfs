package shell

import (
	"io"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
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
	// a converted index reports deletes without their sizes; the ratio is not
	// knowable from here, so it is not listed
	readOnlyConvertedIndex := &master_pb.VolumeInformationMessage{Id: 8, Collection: "a", Size: 100, DeleteCount: 2, DeletedByteCount: 0, ReadOnly: true}

	topo := vacuumTestTopology(
		vacuumTestNode(readOnlyHalfGarbage, writableMostlyGarbage, readOnlyLittleGarbage, readOnlyEmpty, readOnlyAtThreshold, mixedReadOnlyReplica, readOnlyConvertedIndex),
		// the second replica of volume 1 must not list it twice
		vacuumTestNode(readOnlyHalfGarbage, readOnlyOtherCollection, mixedWritableReplica),
	)

	matcherA, err := wildcard.CompileCollectionMatcher("a")
	if err != nil {
		t.Fatalf("compile matcher: %v", err)
	}

	got := readOnlyVolumesAboveThreshold(topo, matcherA, 0.3)
	if want := []uint32{1, 6, 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("collection a, threshold 0.3: got %v, want %v", got, want)
	}

	got = readOnlyVolumesAboveThreshold(topo, nil, 0.3)
	if want := []uint32{1, 4, 6, 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("all collections, threshold 0.3: got %v, want %v", got, want)
	}

	if got := readOnlyVolumesAboveThreshold(topo, matcherA, 0.95); len(got) != 0 {
		t.Fatalf("threshold 0.95: got %v, want none", got)
	}
}

func mustMatcher(t *testing.T, filter string) *wildcard.CollectionMatcher {
	t.Helper()
	m, err := wildcard.CompileCollectionMatcher(filter)
	if err != nil {
		t.Fatalf("compile %q: %v", filter, err)
	}
	return m
}

func TestVacuumRequestsForPattern(t *testing.T) {
	node := vacuumTestNode(
		&master_pb.VolumeInformationMessage{Id: 1, Collection: "a", Size: 100},
		&master_pb.VolumeInformationMessage{Id: 2, Collection: "", Size: 100},
		// read-only volumes stay out of the unnamed-collection sweep, the
		// same as the server skips them on a named-collection sweep — the
		// unreachable address keeps the low-disk probe a fast miss
		&master_pb.VolumeInformationMessage{Id: 3, Collection: "", Size: 100, ReadOnly: true},
		&master_pb.VolumeInformationMessage{Id: 4, Collection: "b", Size: 100},
	)
	node.Id = "localhost:1"
	topo := vacuumTestTopology(node)

	// A named collection sweeps as a single collection-scoped request.
	reqs, err := vacuumRequestsForPattern(nil, io.Discard, topo, mustMatcher(t, "a"))
	if err != nil {
		t.Fatalf("pattern a: %v", err)
	}
	if len(reqs) != 1 || reqs[0].Collection != "a" || reqs[0].VolumeId != 0 {
		t.Fatalf("pattern a: got %v, want one collection-a sweep request", reqs)
	}

	// Patterns cover several named collections.
	reqs, err = vacuumRequestsForPattern(nil, io.Discard, topo, mustMatcher(t, "a,b"))
	if err != nil {
		t.Fatalf("pattern a,b: %v", err)
	}
	names := map[string]bool{}
	for _, r := range reqs {
		names[r.Collection] = true
	}
	if len(reqs) != 2 || !names["a"] || !names["b"] {
		t.Fatalf("pattern a,b: got %v, want sweeps for a and b", reqs)
	}

	// The unnamed collection resolves to per-volume requests — no Collection
	// value selects it alone, and the read-only volume is skipped because the
	// probe cannot confirm low disk space.
	reqs, err = vacuumRequestsForPattern(nil, io.Discard, topo, mustMatcher(t, CollectionDefault))
	if err != nil {
		t.Fatalf("%s: %v", CollectionDefault, err)
	}
	if len(reqs) != 1 || reqs[0].VolumeId != 2 || reqs[0].Collection != "" {
		t.Fatalf("%s: got %v, want one per-volume request for volume 2", CollectionDefault, reqs)
	}

	// A filter hitting nothing is an error, not a silent no-op.
	if _, err = vacuumRequestsForPattern(nil, io.Discard, topo, mustMatcher(t, "missing")); err == nil {
		t.Fatal("expected an error for a filter matching no collection")
	}
}

func TestVacuumRequestsForVolumeIds(t *testing.T) {
	topo := vacuumTestTopology(
		vacuumTestNode(
			&master_pb.VolumeInformationMessage{Id: 1, Collection: "a"},
			&master_pb.VolumeInformationMessage{Id: 2, Collection: ""},
		),
	)

	// A matching name scopes the request to that collection.
	reqs, err := vacuumRequestsForVolumeIds(topo, mustMatcher(t, "a"), []uint32{1})
	if err != nil {
		t.Fatalf("vid 1 in a: %v", err)
	}
	if len(reqs) != 1 || reqs[0].VolumeId != 1 || reqs[0].Collection != "a" {
		t.Fatalf("vid 1 in a: got %v, want {VolumeId:1, Collection:a}", reqs)
	}

	// The unnamed collection sends a volume-id-only request.
	reqs, err = vacuumRequestsForVolumeIds(topo, mustMatcher(t, CollectionDefault), []uint32{2})
	if err != nil {
		t.Fatalf("vid 2 in default: %v", err)
	}
	if len(reqs) != 1 || reqs[0].VolumeId != 2 || reqs[0].Collection != "" {
		t.Fatalf("vid 2 in default: got %v, want {VolumeId:2}", reqs)
	}

	// A volume outside the filter is an error.
	if _, err = vacuumRequestsForVolumeIds(topo, mustMatcher(t, "a"), []uint32{2}); err == nil {
		t.Fatal("expected an error for a volume outside the filter")
	}
}
