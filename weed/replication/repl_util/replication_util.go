package repl_util

import (
	"context"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func CopyFromChunkViews(chunkViews *filer.IntervalList[*filer.ChunkView], filerSource *source.FilerSource, writeFunc func(data []byte) error) error {
	return CopyFromChunkViewsWithEntry(chunkViews, filerSource, writeFunc, nil)
}

// CopyFromChunkViewsWithEntry copies chunk data with optional SSE decryption.
// If entry has SSE-encrypted chunks, data is decrypted before writing.
func CopyFromChunkViewsWithEntry(chunkViews *filer.IntervalList[*filer.ChunkView], filerSource *source.FilerSource, writeFunc func(data []byte) error, entry *filer_pb.Entry) error {
	if entry != nil && detectSSEType(entry) != filer_pb.SSEType_NONE {
		return copyWithDecryption(filerSource, entry, writeFunc)
	}
	return copyChunkViews(chunkViews, filerSource, writeFunc)
}

func copyWithDecryption(filerSource *source.FilerSource, entry *filer_pb.Entry, writeFunc func(data []byte) error) error {
	reader := filer.NewFileReader(filerSource, entry)
	decrypted, err := MaybeDecryptReader(reader, entry)
	if err != nil {
		return err
	}
	buf := make([]byte, 128*1024)
	for {
		n, readErr := decrypted.Read(buf)
		if n > 0 {
			if writeErr := writeFunc(buf[:n]); writeErr != nil {
				return writeErr
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func copyChunkViews(chunkViews *filer.IntervalList[*filer.ChunkView], filerSource *source.FilerSource, writeFunc func(data []byte) error) error {

	for x := chunkViews.Front(); x != nil; x = x.Next {
		chunk := x.Value

		fileUrls, err := filerSource.LookupFileId(context.Background(), chunk.FileId)
		if err != nil {
			return err
		}

		var writeErr error
		var shouldRetry bool
		jwt := filer.JwtForVolumeServer(chunk.FileId)

		for _, fileUrl := range fileUrls {
			shouldRetry, err = util_http.ReadUrlAsStream(context.Background(), fileUrl, jwt, chunk.CipherKey, chunk.IsGzipped, chunk.IsFullChunk(), chunk.OffsetInChunk, int(chunk.ViewSize), func(data []byte) {
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
