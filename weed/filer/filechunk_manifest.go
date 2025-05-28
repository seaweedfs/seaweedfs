package filer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

const (
	ManifestBatch = 10000
)

var bytesBufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

func HasChunkManifest(chunks []*filer_pb.FileChunk) bool {
	for _, chunk := range chunks {
		if chunk.IsChunkManifest {
			return true
		}
	}
	return false
}

func SeparateManifestChunks(chunks []*filer_pb.FileChunk) (manifestChunks, nonManifestChunks []*filer_pb.FileChunk) {
	for _, c := range chunks {
		if c.IsChunkManifest {
			manifestChunks = append(manifestChunks, c)
		} else {
			nonManifestChunks = append(nonManifestChunks, c)
		}
	}
	return
}

func ResolveChunkManifest(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk, startOffset, stopOffset int64) (dataChunks, manifestChunks []*filer_pb.FileChunk, manifestResolveErr error) {
	// TODO maybe parallel this
	for _, chunk := range chunks {

		if max(chunk.Offset, startOffset) >= min(chunk.Offset+int64(chunk.Size), stopOffset) {
			continue
		}

		if !chunk.IsChunkManifest {
			dataChunks = append(dataChunks, chunk)
			continue
		}

		resolvedChunks, err := ResolveOneChunkManifest(ctx, lookupFileIdFn, chunk)
		if err != nil {
			return dataChunks, nil, err
		}

		manifestChunks = append(manifestChunks, chunk)
		// recursive
		subDataChunks, subManifestChunks, subErr := ResolveChunkManifest(ctx, lookupFileIdFn, resolvedChunks, startOffset, stopOffset)
		if subErr != nil {
			return dataChunks, nil, subErr
		}
		dataChunks = append(dataChunks, subDataChunks...)
		manifestChunks = append(manifestChunks, subManifestChunks...)
	}
	return
}

func ResolveOneChunkManifest(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, chunk *filer_pb.FileChunk) (dataChunks []*filer_pb.FileChunk, manifestResolveErr error) {
	if !chunk.IsChunkManifest {
		return
	}

	// IsChunkManifest
	bytesBuffer := bytesBufferPool.Get().(*bytes.Buffer)
	bytesBuffer.Reset()
	defer bytesBufferPool.Put(bytesBuffer)
	err := fetchWholeChunk(ctx, bytesBuffer, lookupFileIdFn, chunk.GetFileIdString(), chunk.CipherKey, chunk.IsCompressed)
	if err != nil {
		return nil, fmt.Errorf("fail to read manifest %s: %v", chunk.GetFileIdString(), err)
	}
	m := &filer_pb.FileChunkManifest{}
	if err := proto.Unmarshal(bytesBuffer.Bytes(), m); err != nil {
		return nil, fmt.Errorf("fail to unmarshal manifest %s: %v", chunk.GetFileIdString(), err)
	}

	// recursive
	filer_pb.AfterEntryDeserialization(m.Chunks)
	return m.Chunks, nil
}

// TODO fetch from cache for weed mount?
func fetchWholeChunk(ctx context.Context, bytesBuffer *bytes.Buffer, lookupFileIdFn wdclient.LookupFileIdFunctionType, fileId string, cipherKey []byte, isGzipped bool) error {
	urlStrings, err := lookupFileIdFn(ctx, fileId)
	if err != nil {
		glog.Errorf("operation LookupFileId %s failed, err: %v", fileId, err)
		return err
	}
	err = retriedStreamFetchChunkData(ctx, bytesBuffer, urlStrings, "", cipherKey, isGzipped, true, 0, 0)
	if err != nil {
		return err
	}
	return nil
}

func fetchChunkRange(buffer []byte, lookupFileIdFn wdclient.LookupFileIdFunctionType, fileId string, cipherKey []byte, isGzipped bool, offset int64) (int, error) {
	urlStrings, err := lookupFileIdFn(context.Background(), fileId)
	if err != nil {
		glog.Errorf("operation LookupFileId %s failed, err: %v", fileId, err)
		return 0, err
	}
	return util_http.RetriedFetchChunkData(context.Background(), buffer, urlStrings, cipherKey, isGzipped, false, offset)
}

func retriedStreamFetchChunkData(ctx context.Context, writer io.Writer, urlStrings []string, jwt string, cipherKey []byte, isGzipped bool, isFullChunk bool, offset int64, size int) (err error) {

	var shouldRetry bool
	var totalWritten int

	for waitTime := time.Second; waitTime < util.RetryWaitTime; waitTime += waitTime / 2 {
		retriedCnt := 0
		for _, urlString := range urlStrings {
			retriedCnt++
			var localProcessed int
			var writeErr error
			shouldRetry, err = util_http.ReadUrlAsStreamAuthenticated(ctx, urlString+"?readDeleted=true", jwt, cipherKey, isGzipped, isFullChunk, offset, size, func(data []byte) {
				if totalWritten > localProcessed {
					toBeSkipped := totalWritten - localProcessed
					if len(data) <= toBeSkipped {
						localProcessed += len(data)
						return // skip if already processed
					}
					data = data[toBeSkipped:]
					localProcessed += toBeSkipped
				}
				var writtenCount int
				writtenCount, writeErr = writer.Write(data)
				localProcessed += writtenCount
				totalWritten += writtenCount
			})
			if !shouldRetry {
				break
			}
			if writeErr != nil {
				err = writeErr
				break
			}
			if err != nil {
				glog.V(0).Infof("read %s failed, err: %v", urlString, err)
			} else {
				break
			}
		}
		// all nodes have tried it
		if retriedCnt == len(urlStrings) {
			break
		}
		if err != nil && shouldRetry {
			glog.V(0).Infof("retry reading in %v", waitTime)
			time.Sleep(waitTime)
		} else {
			break
		}
	}

	return err

}

func MaybeManifestize(saveFunc SaveDataAsChunkFunctionType, inputChunks []*filer_pb.FileChunk) (chunks []*filer_pb.FileChunk, err error) {
	return doMaybeManifestize(saveFunc, inputChunks, ManifestBatch, mergeIntoManifest)
}

func doMaybeManifestize(saveFunc SaveDataAsChunkFunctionType, inputChunks []*filer_pb.FileChunk, mergeFactor int, mergefn func(saveFunc SaveDataAsChunkFunctionType, dataChunks []*filer_pb.FileChunk) (manifestChunk *filer_pb.FileChunk, err error)) (chunks []*filer_pb.FileChunk, err error) {

	var dataChunks []*filer_pb.FileChunk
	for _, chunk := range inputChunks {
		if !chunk.IsChunkManifest {
			dataChunks = append(dataChunks, chunk)
		} else {
			chunks = append(chunks, chunk)
		}
	}

	remaining := len(dataChunks)
	for i := 0; i+mergeFactor <= len(dataChunks); i += mergeFactor {
		chunk, err := mergefn(saveFunc, dataChunks[i:i+mergeFactor])
		if err != nil {
			return dataChunks, err
		}
		chunks = append(chunks, chunk)
		remaining -= mergeFactor
	}
	// remaining
	for i := len(dataChunks) - remaining; i < len(dataChunks); i++ {
		chunks = append(chunks, dataChunks[i])
	}
	return
}

func mergeIntoManifest(saveFunc SaveDataAsChunkFunctionType, dataChunks []*filer_pb.FileChunk) (manifestChunk *filer_pb.FileChunk, err error) {

	filer_pb.BeforeEntrySerialization(dataChunks)

	// create and serialize the manifest
	data, serErr := proto.Marshal(&filer_pb.FileChunkManifest{
		Chunks: dataChunks,
	})
	if serErr != nil {
		return nil, fmt.Errorf("serializing manifest: %v", serErr)
	}

	minOffset, maxOffset := int64(math.MaxInt64), int64(math.MinInt64)
	for _, chunk := range dataChunks {
		if minOffset > int64(chunk.Offset) {
			minOffset = chunk.Offset
		}
		if maxOffset < int64(chunk.Size)+chunk.Offset {
			maxOffset = int64(chunk.Size) + chunk.Offset
		}
	}

	manifestChunk, err = saveFunc(bytes.NewReader(data), "", 0, 0)
	if err != nil {
		return nil, err
	}
	manifestChunk.IsChunkManifest = true
	manifestChunk.Offset = minOffset
	manifestChunk.Size = uint64(maxOffset - minOffset)

	return
}

type SaveDataAsChunkFunctionType func(reader io.Reader, name string, offset int64, tsNs int64) (chunk *filer_pb.FileChunk, err error)
