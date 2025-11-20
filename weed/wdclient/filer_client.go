package wdclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// FilerClient provides volume location services by querying a filer
// It uses the shared vidMap cache for efficient lookups
type FilerClient struct {
	*vidMapClient
	filerAddress   pb.ServerAddress
	grpcDialOption grpc.DialOption
}

// filerVolumeProvider implements VolumeLocationProvider by querying filer
type filerVolumeProvider struct {
	filerAddress   pb.ServerAddress
	grpcDialOption grpc.DialOption
}

// NewFilerClient creates a new client that queries filer for volume locations
func NewFilerClient(filerAddress pb.ServerAddress, grpcDialOption grpc.DialOption, dataCenter string) *FilerClient {
	provider := &filerVolumeProvider{
		filerAddress:   filerAddress,
		grpcDialOption: grpcDialOption,
	}

	return &FilerClient{
		vidMapClient:   newVidMapClient(provider, dataCenter),
		filerAddress:   filerAddress,
		grpcDialOption: grpcDialOption,
	}
}

// LookupVolumeIds queries the filer for volume locations
func (p *filerVolumeProvider) LookupVolumeIds(ctx context.Context, volumeIds []string) (map[string][]Location, error) {
	result := make(map[string][]Location)

	err := pb.WithGrpcFilerClient(false, 0, p.filerAddress, p.grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.LookupVolume(ctx, &filer_pb.LookupVolumeRequest{
			VolumeIds: volumeIds,
		})
		if err != nil {
			return fmt.Errorf("filer.LookupVolume failed: %w", err)
		}

		for vid, locs := range resp.LocationsMap {
			var locations []Location
			for _, loc := range locs.Locations {
				locations = append(locations, Location{
					Url:        loc.Url,
					PublicUrl:  loc.PublicUrl,
					DataCenter: loc.DataCenter,
					GrpcPort:   int(loc.GrpcPort),
				})
			}
			if len(locations) > 0 {
				result[vid] = locations
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	glog.V(3).Infof("FilerClient: looked up %d volumes, found %d", len(volumeIds), len(result))
	return result, nil
}

