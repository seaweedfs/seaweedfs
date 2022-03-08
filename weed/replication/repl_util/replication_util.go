package repl_util

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func CopyFromChunkViews(chunkViews []*filer.ChunkView, filerSource *source.FilerSource, writeFunc func(data []byte) error) error {

	for _, chunk := range chunkViews {

		fileUrls, err := filerSource.LookupFileId(chunk.FileId)
		if err != nil {
			return err
		}

		var writeErr error
		var shouldRetry bool

		for _, fileUrl := range fileUrls {
			shouldRetry, err = util.ReadUrlAsStream(fileUrl, chunk.CipherKey, chunk.IsGzipped, chunk.IsFullChunk(), chunk.Offset, int(chunk.Size), func(data []byte) {
				writeErr = writeFunc(data)
			})
			if err != nil {
				glog.V(1).Infof("read from %s: %v", fileUrl, err)
			} else if writeErr != nil {
				glog.V(1).Infof("copy from %s: %v", fileUrl, writeErr)
			} else {
				break
			}
		}
		if shouldRetry && err != nil {
			return err
		}
		if writeErr != nil {
			return writeErr
		}
	}
	return nil
}
