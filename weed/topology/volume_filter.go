package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeFilter narrows a topology listing to the volumes a caller asked about,
// selecting only what is listed under a disk. A nil field filters nothing, and
// only nil does: the empty collection is a real one.
type VolumeFilter struct {
	Collection *string
	VolumeId   *needle.VolumeId
}

// NewVolumeFilter reads what a VolumeList request asked for, where empty and
// zero mean everything so a caller that forgets to narrow gets too much rather
// than the wrong thing.
func NewVolumeFilter(req *master_pb.VolumeListRequest) VolumeFilter {
	var filter VolumeFilter
	switch {
	case req.Collection != "":
		collection := req.Collection
		filter.Collection = &collection
	case req.DefaultCollectionOnly:
		defaultCollection := ""
		filter.Collection = &defaultCollection
	}
	if req.VolumeId != 0 {
		volumeId := needle.VolumeId(req.VolumeId)
		filter.VolumeId = &volumeId
	}
	return filter
}

// SelectsEverything lets a caller size its result for the whole disk up front.
func (f VolumeFilter) SelectsEverything() bool {
	return f.Collection == nil && f.VolumeId == nil
}

func (f VolumeFilter) matches(collection string, id needle.VolumeId) bool {
	if f.Collection != nil && *f.Collection != collection {
		return false
	}
	if f.VolumeId != nil && *f.VolumeId != id {
		return false
	}
	return true
}
