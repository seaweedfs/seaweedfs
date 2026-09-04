package shell

import (
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func newMergeCmd(limitMB uint64, vols ...*master_pb.VolumeInformationMessage) *commandFsMergeVolumes {
	c := &commandFsMergeVolumes{
		volumes:         make(map[needle.VolumeId]*master_pb.VolumeInformationMessage),
		volumeSizeLimit: limitMB * 1024 * 1024,
	}
	for _, v := range vols {
		c.volumes[needle.VolumeId(v.Id)] = v
	}
	return c
}

// An explicit -fromVolumeId/-toVolumeId pair must be honored as given, even when
// the source is larger than the target. The heuristic planner only ever merges
// a smaller volume into a larger one, which used to make this request a silent
// no-op.
func TestCreateMergePlan_HonorsExplicitDirection(t *testing.T) {
	larger := &master_pb.VolumeInformationMessage{Id: 87, Size: 7192590976}
	smaller := &master_pb.VolumeInformationMessage{Id: 83, Size: 7088822248}
	c := newMergeCmd(250000, larger, smaller)

	plan, err := c.createMergePlan("*", []needle.VolumeId{83}, needle.VolumeId(87))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := plan.targets[needle.VolumeId(87)]; len(got) != 1 || got[0] != needle.VolumeId(83) {
		t.Fatalf("expected 87->83, got plan=%v", plan.targets)
	}

	// The reverse direction keeps working too.
	plan, err = c.createMergePlan("*", []needle.VolumeId{87}, needle.VolumeId(83))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := plan.targets[needle.VolumeId(83)]; len(got) != 1 || got[0] != needle.VolumeId(87) {
		t.Fatalf("expected 83->87, got plan=%v", plan.targets)
	}
}

// Merging into an empty target is valid (e.g. consolidating into a freshly
// vacuumed volume); only an empty source should be rejected.
func TestCreateMergePlan_DirectedAllowsEmptyTarget(t *testing.T) {
	from := &master_pb.VolumeInformationMessage{Id: 87, Size: 100}
	emptyTo := &master_pb.VolumeInformationMessage{Id: 83, Size: 0}
	c := newMergeCmd(250000, from, emptyTo)

	plan, err := c.createMergePlan("*", []needle.VolumeId{83}, needle.VolumeId(87))
	if err != nil {
		t.Fatalf("unexpected error merging into empty target: %v", err)
	}
	if got := plan.targets[needle.VolumeId(87)]; len(got) != 1 || got[0] != needle.VolumeId(83) {
		t.Fatalf("expected 87->83, got plan=%v", plan.targets)
	}
}

// The directed planner rejects a self-map on its own, not just via Do, since it
// is exercised directly.
func TestCreateMergePlan_DirectedRejectsSelfMap(t *testing.T) {
	v := &master_pb.VolumeInformationMessage{Id: 87, Size: 100}
	c := newMergeCmd(250000, v)

	_, err := c.createMergePlan("*", []needle.VolumeId{87}, needle.VolumeId(87))
	if err == nil || !strings.Contains(err.Error(), "no volume id changes") {
		t.Fatalf("expected self-map rejection, got %v", err)
	}
}

func TestCreateMergePlan_DirectedRejectsIneligible(t *testing.T) {
	cases := []struct {
		name     string
		from, to *master_pb.VolumeInformationMessage
		coll     string
		wantErr  string
	}{
		{
			name:    "readonly source",
			from:    &master_pb.VolumeInformationMessage{Id: 87, Size: 100, ReadOnly: true},
			to:      &master_pb.VolumeInformationMessage{Id: 83, Size: 100},
			coll:    "*",
			wantErr: "volume 87 is readonly",
		},
		{
			name:    "readonly target",
			from:    &master_pb.VolumeInformationMessage{Id: 87, Size: 100},
			to:      &master_pb.VolumeInformationMessage{Id: 83, Size: 100, ReadOnly: true},
			coll:    "*",
			wantErr: "volume 83 is readonly",
		},
		{
			name:    "empty source",
			from:    &master_pb.VolumeInformationMessage{Id: 87, Size: 0},
			to:      &master_pb.VolumeInformationMessage{Id: 83, Size: 100},
			coll:    "*",
			wantErr: "volume 87 is empty",
		},
		{
			name:    "wrong collection",
			from:    &master_pb.VolumeInformationMessage{Id: 87, Size: 100, Collection: "other"},
			to:      &master_pb.VolumeInformationMessage{Id: 83, Size: 100, Collection: "other"},
			coll:    "wanted",
			wantErr: "volume 87 is not in collection",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newMergeCmd(250000, tc.from, tc.to)
			_, err := c.createMergePlan(tc.coll, []needle.VolumeId{needle.VolumeId(tc.to.Id)}, needle.VolumeId(tc.from.Id))
			if err == nil {
				t.Fatalf("expected error %q, got nil", tc.wantErr)
			}
			if got := err.Error(); !strings.Contains(got, tc.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tc.wantErr, got)
			}
		})
	}
}

// A source that fits into no single target distributes across several, each
// chunk going to the target with the most remaining capacity.
func TestCreateMergePlan_DistributesAcrossMultipleTargets(t *testing.T) {
	mb := uint64(1024 * 1024)
	from := &master_pb.VolumeInformationMessage{Id: 112, Size: 60 * mb}
	to1 := &master_pb.VolumeInformationMessage{Id: 111, Size: 60 * mb}
	to2 := &master_pb.VolumeInformationMessage{Id: 107, Size: 70 * mb}
	c := newMergeCmd(100, from, to1, to2)

	plan, err := c.createMergePlan("*", []needle.VolumeId{111, 107}, needle.VolumeId(112))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := plan.targets[needle.VolumeId(112)]; len(got) != 2 {
		t.Fatalf("expected two targets, got %v", got)
	}

	// 111 has 40 MB free, 107 has 30 MB free. The first chunk goes to the
	// emptier 111; allocations keep balancing remaining capacity after that.
	vid, ok := plan.allocate(needle.VolumeId(112), 5*mb)
	if !ok || vid != needle.VolumeId(111) {
		t.Fatalf("expected first chunk on 111, got %v ok=%v", vid, ok)
	}
	vid, ok = plan.allocate(needle.VolumeId(112), 10*mb)
	if !ok || vid != needle.VolumeId(111) {
		t.Fatalf("expected second chunk on 111 (35 MB free vs 30), got %v ok=%v", vid, ok)
	}
	vid, ok = plan.allocate(needle.VolumeId(112), 5*mb)
	if !ok || vid != needle.VolumeId(107) {
		t.Fatalf("expected third chunk on 107 (30 MB free vs 25), got %v ok=%v", vid, ok)
	}
	// A chunk larger than any remaining capacity is refused.
	if vid, ok = plan.allocate(needle.VolumeId(112), 45*mb); ok {
		t.Fatalf("expected no room for oversized chunk, got %v", vid)
	}
	// Smaller chunks still fit afterwards.
	if _, ok = plan.allocate(needle.VolumeId(112), 20*mb); !ok {
		t.Fatal("expected room for 20 MB chunk")
	}

	// A failed move's release makes the reservation reusable.
	vid2, ok := plan.allocate(needle.VolumeId(112), 20*mb)
	if !ok {
		t.Fatal("expected room for second 20 MB chunk")
	}
	plan.release(needle.VolumeId(112), vid2, 20*mb)
	if vid3, ok := plan.allocate(needle.VolumeId(112), 20*mb); !ok || vid3 != vid2 {
		t.Fatalf("released capacity must be reusable, got %v ok=%v", vid3, ok)
	}
}

// The combined free capacity of all targets must cover the source's live data.
func TestCreateMergePlan_MultiTargetRejectsInsufficientCapacity(t *testing.T) {
	mb := uint64(1024 * 1024)
	from := &master_pb.VolumeInformationMessage{Id: 112, Size: 90 * mb}
	to1 := &master_pb.VolumeInformationMessage{Id: 111, Size: 60 * mb}
	to2 := &master_pb.VolumeInformationMessage{Id: 107, Size: 60 * mb}
	c := newMergeCmd(100, from, to1, to2)

	_, err := c.createMergePlan("*", []needle.VolumeId{111, 107}, needle.VolumeId(112))
	if err == nil || !strings.Contains(err.Error(), "cannot merge into volumes") {
		t.Fatalf("expected capacity rejection, got %v", err)
	}
}

// Without -fromVolumeId, a -toVolumeId list restricts the heuristic planner's
// target candidates to the listed volumes.
func TestCreateMergePlan_TargetListRestrictsHeuristic(t *testing.T) {
	mb := uint64(1024 * 1024)
	big := &master_pb.VolumeInformationMessage{Id: 1, Size: 80 * mb}
	mid := &master_pb.VolumeInformationMessage{Id: 2, Size: 50 * mb}
	small := &master_pb.VolumeInformationMessage{Id: 3, Size: 10 * mb}
	c := newMergeCmd(100, big, mid, small)

	plan, err := c.createMergePlan("*", []needle.VolumeId{2}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := plan.targets[needle.VolumeId(3)]; len(got) != 1 || got[0] != needle.VolumeId(2) {
		t.Fatalf("expected 3->2, got %v", plan.targets)
	}
	if plan.isSource(needle.VolumeId(1)) {
		t.Fatalf("volume 1 must not merge anywhere, got %v", plan.targets)
	}
}

func TestParseTargetVolumeIds(t *testing.T) {
	if ids, err := parseTargetVolumeIds(""); err != nil || ids != nil {
		t.Fatalf("empty arg: got %v, %v", ids, err)
	}
	if ids, err := parseTargetVolumeIds("0"); err != nil || ids != nil {
		t.Fatalf("zero arg: got %v, %v", ids, err)
	}
	ids, err := parseTargetVolumeIds("111, 107")
	if err != nil || len(ids) != 2 || ids[0] != needle.VolumeId(111) || ids[1] != needle.VolumeId(107) {
		t.Fatalf("list arg: got %v, %v", ids, err)
	}
	if _, err = parseTargetVolumeIds("111,111"); err == nil {
		t.Fatal("expected duplicate rejection")
	}
	if _, err = parseTargetVolumeIds("111,x"); err == nil {
		t.Fatal("expected invalid id rejection")
	}
	if _, err = parseTargetVolumeIds("4294967297"); err == nil {
		t.Fatal("expected uint32 overflow rejection")
	}
}

// A volume reporting more deleted bytes than it holds must read as empty.
func TestGetVolumeSize_ClampsDeletedOverSize(t *testing.T) {
	c := newMergeCmd(250000)

	overDeleted := &master_pb.VolumeInformationMessage{Id: 1, Size: 1000, DeletedByteCount: 4000}
	if got := c.getVolumeSize(overDeleted); got != 0 {
		t.Errorf("expected 0 for a volume with more deleted than stored, got %d", got)
	}

	healthy := &master_pb.VolumeInformationMessage{Id: 2, Size: 3000, DeletedByteCount: 1000}
	if got := c.getVolumeSize(healthy); got != 2000 {
		t.Errorf("expected 2000, got %d", got)
	}
}

// Manifests on volumes in a foreign collection cannot reference plan volumes
// and must be skipped instead of downloaded; plan sources and unknown volumes
// must still be resolved.
func TestManifestMayReferencePlan(t *testing.T) {
	src := &master_pb.VolumeInformationMessage{Id: 1, Size: 100, Collection: "x"}
	same := &master_pb.VolumeInformationMessage{Id: 2, Size: 100, Collection: "x"}
	foreign := &master_pb.VolumeInformationMessage{Id: 3, Size: 100, Collection: "y"}
	c := newMergeCmd(250000, src, same, foreign)

	plan := newMergePlan(c.volumeSizeLimit)
	plan.targets[needle.VolumeId(1)] = []needle.VolumeId{2}
	planCollections := map[string]bool{"x": true}

	for vid, want := range map[needle.VolumeId]bool{
		1: true,  // plan source
		2: true,  // same collection
		3: false, // foreign collection
		9: true,  // unknown volume, resolve conservatively
	} {
		if got := c.manifestMayReferencePlan(plan, planCollections, vid); got != want {
			t.Fatalf("volume %d: got %v, want %v", vid, got, want)
		}
	}
}
