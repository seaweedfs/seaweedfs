package repl_util

import (
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func CopyFromChunkViews(chunkViews *filer.IntervalList[*filer.ChunkView], filerSource *source.FilerSource, writeFunc func(data []byte) error) error {

	for x := chunkViews.Front(); x != nil; x = x.Next {
		chunk := x.Value

		fileUrls, err := filerSource.LookupFileId(chunk.FileId)
		if err != nil {
			return err
		}

		var writeErr error
		var shouldRetry bool

		for _, fileUrl := range fileUrls {
			shouldRetry, err = util_http.ReadUrlAsStream(fileUrl, chunk.CipherKey, chunk.IsGzipped, chunk.IsFullChunk(), chunk.OffsetInChunk, int(chunk.ViewSize), func(data []byte) {
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
