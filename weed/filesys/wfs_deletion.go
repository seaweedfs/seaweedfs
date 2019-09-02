package filesys

import (
	"context"

	"github.com/joeslay/seaweedfs/weed/filer2"
	"github.com/joeslay/seaweedfs/weed/glog"
	"github.com/joeslay/seaweedfs/weed/operation"
	"github.com/joeslay/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

func (wfs *WFS) deleteFileChunks(ctx context.Context, chunks []*filer_pb.FileChunk) {
	if len(chunks) == 0 {
		return
	}

	var fileIds []string
	for _, chunk := range chunks {
		fileIds = append(fileIds, chunk.GetFileIdString())
	}

	wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {
		deleteFileIds(ctx, wfs.option.GrpcDialOption, client, fileIds)
		return nil
	})
}

func deleteFileIds(ctx context.Context, grpcDialOption grpc.DialOption, client filer_pb.SeaweedFilerClient, fileIds []string) error {

	var vids []string
	for _, fileId := range fileIds {
		vids = append(vids, filer2.VolumeId(fileId))
	}

	lookupFunc := func(vids []string) (map[string]operation.LookupResult, error) {

		m := make(map[string]operation.LookupResult)

		glog.V(4).Infof("remove file lookup volume id locations: %v", vids)
		resp, err := client.LookupVolume(ctx, &filer_pb.LookupVolumeRequest{
			VolumeIds: vids,
		})
		if err != nil {
			return m, err
		}

		for _, vid := range vids {
			lr := operation.LookupResult{
				VolumeId:  vid,
				Locations: nil,
			}
			locations := resp.LocationsMap[vid]
			for _, loc := range locations.Locations {
				lr.Locations = append(lr.Locations, operation.Location{
					Url:       loc.Url,
					PublicUrl: loc.PublicUrl,
				})
			}
			m[vid] = lr
		}

		return m, err
	}

	_, err := operation.DeleteFilesWithLookupVolumeId(grpcDialOption, fileIds, lookupFunc)

	return err
}
