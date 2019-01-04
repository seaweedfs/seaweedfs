package filesys

import (
	"context"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (wfs *WFS) loopProcessingDeletion() {

	ticker := time.NewTicker(2 * time.Second)

	wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		var fileIds []string
		for {
			select {
			case fids := <-wfs.fileIdsDeletionChan:
				fileIds = append(fileIds, fids...)
				if len(fileIds) >= 1024 {
					glog.V(1).Infof("deleting fileIds len=%d", len(fileIds))
					deleteFileIds(context.Background(), client, fileIds)
					fileIds = fileIds[:0]
				}
			case <-ticker.C:
				if len(fileIds) > 0 {
					glog.V(1).Infof("timed deletion fileIds len=%d", len(fileIds))
					deleteFileIds(context.Background(), client, fileIds)
					fileIds = fileIds[:0]
				}
			}
		}
	})

}

func (wfs *WFS) asyncDeleteFileChunks(chunks []*filer_pb.FileChunk) {
	if len(chunks) > 0 {
		var fileIds []string
		for _, chunk := range chunks {
			fileIds = append(fileIds, chunk.FileId)
		}
		wfs.fileIdsDeletionChan <- fileIds
	}
}
