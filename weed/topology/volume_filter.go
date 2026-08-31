package topology

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

// VolumeFilter narrows a topology listing to the volumes a caller asked about,
// selecting only what is listed under a disk. A nil field filters nothing, and
// only nil does: the empty collection is a real one.
type VolumeFilter struct {
	Collection        *string
	remoteStorageName *string
	// VolumeIds selects the volumes it holds, and an empty one selects them all.
	VolumeIds map[needle.VolumeId]struct{}
	// nothing selects the topology alone, for a listing whose volumes travel
	// in messages of their own.
	nothing bool
}

// NoVolumes selects the topology and no volume in it.
func NoVolumes() VolumeFilter {
	return VolumeFilter{nothing: true}
}

// NewVolumeFilter reads what a VolumeList request asked for, where empty and
// zero mean everything so a caller that forgets to narrow gets too much rather
// than the wrong thing.
func NewVolumeFilter(req *master_pb.VolumeListRequest) VolumeFilter {
	var filter VolumeFilter

	if req.WithoutVolumes {
		filter.nothing = true
		return filter
	}

	switch {
	case req.Collection != "":
		collection := req.Collection
		filter.Collection = &collection
	case req.DefaultCollectionOnly:
		defaultCollection := ""
		filter.Collection = &defaultCollection
	}

	switch {
	case req.RemoteStorageName != "":
		filter.remoteStorageName = new(req.RemoteStorageName)
	case req.LocalVolumeOnly:
		filter.remoteStorageName = new("")
	}

	if len(req.VolumeIds) > 0 {
		filter.VolumeIds = make(map[needle.VolumeId]struct{}, len(req.VolumeIds))
		for _, volumeId := range req.VolumeIds {
			filter.VolumeIds[needle.VolumeId(volumeId)] = struct{}{}
		}
	}
	return filter
}

// SelectsEverything lets a caller size its result for the whole disk up front.
func (f VolumeFilter) SelectsEverything() bool {
	return !f.nothing && f.Collection == nil && len(f.VolumeIds) == 0 && f.remoteStorageName == nil
}

type volumeLike interface {
	GetCollection() string
	GetVolumeId() needle.VolumeId
	GetRemoteStorageName() string
}

func (f VolumeFilter) matches(vi volumeLike) bool {
	if f.nothing {
		return false
	}
	if f.Collection != nil && *f.Collection != vi.GetCollection() {
		return false
	}
	if f.remoteStorageName != nil {
		pattern, name := *f.remoteStorageName, vi.GetRemoteStorageName()
		// the empty name is the local volumes, which only asking for the empty
		// name selects; even * leaves them out.
		if name == "" && pattern != "" {
			return false
		}
		if !wildcard.MatchesWildcard(pattern, name) {
			return false
		}
	}
	if len(f.VolumeIds) > 0 {
		if _, ok := f.VolumeIds[vi.GetVolumeId()]; !ok {
			return false
		}
	}
	return true
}
