package filesys

import (
	"context"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/util/log"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (wfs *WFS) deleteFileChunks(chunks []*filer_pb.FileChunk) {
	if len(chunks) == 0 {
		return
	}

	var fileIds []string
	for _, chunk := range chunks {
		if !chunk.IsChunkManifest {
			fileIds = append(fileIds, chunk.GetFileIdString())
			continue
		}
		dataChunks, manifestResolveErr := filer.ResolveOneChunkManifest(filer.LookupFn(wfs), chunk)
		if manifestResolveErr != nil {
			log.Infof("failed to resolve manifest %s: %v", chunk.FileId, manifestResolveErr)
		}
		for _, dChunk := range dataChunks {
			fileIds = append(fileIds, dChunk.GetFileIdString())
		}
		fileIds = append(fileIds, chunk.GetFileIdString())
	}

	wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		wfs.deleteFileIds(wfs.option.GrpcDialOption, client, fileIds)
		return nil
	})
}

func (wfs *WFS) deleteFileIds(grpcDialOption grpc.DialOption, client filer_pb.SeaweedFilerClient, fileIds []string) error {

	var vids []string
	for _, fileId := range fileIds {
		vids = append(vids, filer.VolumeId(fileId))
	}

	lookupFunc := func(vids []string) (map[string]operation.LookupResult, error) {

		m := make(map[string]operation.LookupResult)

		log.Tracef("deleteFileIds lookup volume id locations: %v", vids)
		resp, err := client.LookupVolume(context.Background(), &filer_pb.LookupVolumeRequest{
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
			locations, found := resp.LocationsMap[vid]
			if !found {
				continue
			}
			for _, loc := range locations.Locations {
				lr.Locations = append(lr.Locations, operation.Location{
					Url:       wfs.AdjustedUrl(loc),
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
