package weed_server

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func (ms *MasterServer) LookupVolume(ctx context.Context, req *master_pb.LookupVolumeRequest) (*master_pb.LookupVolumeResponse, error) {
	resp := &master_pb.LookupVolumeResponse{}
	volumeLocations := ms.lookupVolumeId(req.VolumeIds, req.Collection)

	for _, result := range volumeLocations {
		var locations []*master_pb.Location
		for _, loc := range result.Locations {
			locations = append(locations, &master_pb.Location{
				Url:       loc.Url,
				PublicUrl: loc.PublicUrl,
			})
		}
		resp.VolumeIdLocations = append(resp.VolumeIdLocations, &master_pb.LookupVolumeResponse_VolumeIdLocation{
			VolumeId:  result.VolumeId,
			Locations: locations,
			Error:     result.Error,
		})
	}

	return resp, nil
}
