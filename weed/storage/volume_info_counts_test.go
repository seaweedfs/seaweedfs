package storage

import (
	"math"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
)

// The counts are narrowed on the way in, so a report that claims more than a
// volume can hold must pin at the ceiling rather than wrap to a small number.
func TestVolumeInfoCountsDoNotWrap(t *testing.T) {
	for _, tc := range []struct {
		name           string
		reported, want uint64
	}{
		{"an ordinary count", 1000, 1000},
		{"the ceiling", math.MaxUint32, math.MaxUint32},
		{"past the ceiling", math.MaxUint32 + 1, math.MaxUint32},
		{"far past it", 1 << 40, math.MaxUint32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vi, err := NewVolumeInfo(&master_pb.VolumeInformationMessage{
				Id: 1, Version: 3, FileCount: tc.reported, DeleteCount: tc.reported,
			})
			if err != nil {
				t.Fatal(err)
			}
			if uint64(vi.FileCount) != tc.want {
				t.Errorf("file count %d, want %d", vi.FileCount, tc.want)
			}
			if uint64(vi.DeleteCount) != tc.want {
				t.Errorf("delete count %d, want %d", vi.DeleteCount, tc.want)
			}
		})
	}
}

// Deletions are tracked apart from the totals they come off, so a replica can
// transiently report more deleted than it holds. Unsigned counts make that a
// wrap rather than a negative, so it stays guarded.
func TestVolumeInfoSurvivesMoreDeletesThanFiles(t *testing.T) {
	vi, err := NewVolumeInfo(&master_pb.VolumeInformationMessage{
		Id: 1, Version: 3, FileCount: 10, DeleteCount: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if vi.FileCount != 10 || vi.DeleteCount != 50 {
		t.Fatalf("counts not carried across: %d/%d", vi.FileCount, vi.DeleteCount)
	}
	m := vi.ToVolumeInformationMessage()
	if m.FileCount != 10 || m.DeleteCount != 50 {
		t.Errorf("round trip changed the counts: %d/%d", m.FileCount, m.DeleteCount)
	}
}
