package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// reportedVolumes records which disk types a heartbeat named each volume on, so
// a volume that moved between disks of one server is dropped from the disk it
// left rather than lingering there as a second copy.
//
// Disk types are interned to an index: a server reports a handful of them
// across hundreds of thousands of volumes, and a string header per volume would
// cost more than the map holding them.
type reportedVolumes struct {
	diskTypes []string
	byVolume  map[needle.VolumeId]int32
	// extra covers volumes named on more than one disk type, which a server can
	// legitimately do when a stale twin is re-attached on another disk. Stays
	// nil otherwise.
	extra map[needle.VolumeId][]int32
	// duplicated records the same volume named twice on one disk type, which
	// the master stores once and so cannot represent.
	duplicated bool
}

func newReportedVolumes(size int) *reportedVolumes {
	return &reportedVolumes{byVolume: make(map[needle.VolumeId]int32, size)}
}

func (r *reportedVolumes) add(vid needle.VolumeId, diskType string) {
	index := r.internDiskType(diskType)
	existing, seen := r.byVolume[vid]
	if !seen {
		r.byVolume[vid] = index
		return
	}
	if existing == index || r.hasExtra(vid, index) {
		r.duplicated = true
		return
	}
	if r.extra == nil {
		r.extra = make(map[needle.VolumeId][]int32)
	}
	r.extra[vid] = append(r.extra[vid], index)
}

func (r *reportedVolumes) internDiskType(diskType string) int32 {
	if index := r.diskTypeIndex(diskType); index >= 0 {
		return index
	}
	r.diskTypes = append(r.diskTypes, diskType)
	return int32(len(r.diskTypes) - 1)
}

// diskTypeIndex resolves a disk type to its index, or -1 when the heartbeat
// named no volume on it.
func (r *reportedVolumes) diskTypeIndex(diskType string) int32 {
	for i, known := range r.diskTypes {
		if known == diskType {
			return int32(i)
		}
	}
	return -1
}

// namedOn reports whether the heartbeat placed vid on the disk type at index.
func (r *reportedVolumes) namedOn(vid needle.VolumeId, index int32) bool {
	if index < 0 {
		return false
	}
	if stored, ok := r.byVolume[vid]; ok && stored == index {
		return true
	}
	return r.hasExtra(vid, index)
}

func (r *reportedVolumes) hasExtra(vid needle.VolumeId, index int32) bool {
	for _, other := range r.extra[vid] {
		if other == index {
			return true
		}
	}
	return false
}

func (r *reportedVolumes) count() int { return len(r.byVolume) }
