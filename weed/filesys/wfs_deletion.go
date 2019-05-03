package filesys

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (wfs *WFS) deleteFileChunks(ctx context.Context, chunks []*filer_pb.FileChunk) {
	if len(chunks) == 0 {
		return
	}

	var fileIds []string
	for _, chunk := range chunks {
		fileIds = append(fileIds, chunk.FileId)
	}

	wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {
		deleteFileIds(ctx, wfs.option.GrpcDialOption, client, fileIds)
		return nil
	})
}
