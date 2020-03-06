package filer2

import (
	"io"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
)

func StreamContent(masterClient *wdclient.MasterClient, w io.Writer, chunks []*filer_pb.FileChunk, offset int64, size int) error {

	chunkViews := ViewFromChunks(chunks, offset, size)

	fileId2Url := make(map[string]string)

	for _, chunkView := range chunkViews {

		urlString, err := masterClient.LookupFileId(chunkView.FileId)
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
			return err
		}
		fileId2Url[chunkView.FileId] = urlString
	}

	for _, chunkView := range chunkViews {
		urlString := fileId2Url[chunkView.FileId]
		err := util.ReadUrlAsStream(urlString, chunkView.CipherKey, chunkView.IsFullChunk, chunkView.Offset, int(chunkView.Size), func(data []byte) {
			w.Write(data)
		})
		if err != nil {
			glog.V(1).Infof("read %s failed, err: %v", chunkView.FileId, err)
			return err
		}
	}

	return nil

}
