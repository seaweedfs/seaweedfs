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

	plan, err := c.createMergePlan("*", needle.VolumeId(83), needle.VolumeId(87))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := plan[needle.VolumeId(87)]; got != needle.VolumeId(83) {
		t.Fatalf("expected 87->83, got plan=%v", plan)
	}

	// The reverse direction keeps working too.
	plan, err = c.createMergePlan("*", needle.VolumeId(87), needle.VolumeId(83))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := plan[needle.VolumeId(83)]; got != needle.VolumeId(87) {
		t.Fatalf("expected 83->87, got plan=%v", plan)
	}
}

// Merging into an empty target is valid (e.g. consolidating into a freshly
// vacuumed volume); only an empty source should be rejected.
func TestCreateMergePlan_DirectedAllowsEmptyTarget(t *testing.T) {
	from := &master_pb.VolumeInformationMessage{Id: 87, Size: 100}
	emptyTo := &master_pb.VolumeInformationMessage{Id: 83, Size: 0}
	c := newMergeCmd(250000, from, emptyTo)

	plan, err := c.createMergePlan("*", needle.VolumeId(83), needle.VolumeId(87))
	if err != nil {
		t.Fatalf("unexpected error merging into empty target: %v", err)
	}
	if got := plan[needle.VolumeId(87)]; got != needle.VolumeId(83) {
		t.Fatalf("expected 87->83, got plan=%v", plan)
	}
}

// The directed planner rejects a self-map on its own, not just via Do, since it
// is exercised directly.
func TestCreateMergePlan_DirectedRejectsSelfMap(t *testing.T) {
	v := &master_pb.VolumeInformationMessage{Id: 87, Size: 100}
	c := newMergeCmd(250000, v)

	_, err := c.createMergePlan("*", needle.VolumeId(87), needle.VolumeId(87))
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
			_, err := c.createMergePlan(tc.coll, needle.VolumeId(tc.to.Id), needle.VolumeId(tc.from.Id))
			if err == nil {
				t.Fatalf("expected error %q, got nil", tc.wantErr)
			}
			if got := err.Error(); !strings.Contains(got, tc.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tc.wantErr, got)
			}
		})
	}
}
