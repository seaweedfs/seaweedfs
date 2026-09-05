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

func ResolveChunkManifest(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, chunks []*filer_pb.FileChunk, startOffset, stopOffset int64, invalidator CacheInvalidator) (dataChunks, manifestChunks []*filer_pb.FileChunk, manifestResolveErr error) {
	// TODO maybe parallel this
	for _, chunk := range chunks {

		if max(chunk.Offset, startOffset) >= min(chunk.Offset+int64(chunk.Size), stopOffset) {
			continue
		}

		if !chunk.IsChunkManifest {
			dataChunks = append(dataChunks, chunk)
			continue
		}

		resolvedChunks, err := ResolveOneChunkManifest(ctx, lookupFileIdFn, chunk, invalidator)
		if err != nil {
			return dataChunks, nil, err
		}

		manifestChunks = append(manifestChunks, chunk)
		// recursive
		subDataChunks, subManifestChunks, subErr := ResolveChunkManifest(ctx, lookupFileIdFn, resolvedChunks, startOffset, stopOffset, invalidator)
		if subErr != nil {
			return dataChunks, nil, subErr
		}
		dataChunks = append(dataChunks, subDataChunks...)
		manifestChunks = append(manifestChunks, subManifestChunks...)
	}
	return
}

func ResolveOneChunkManifest(ctx context.Context, lookupFileIdFn wdclient.LookupFileIdFunctionType, chunk *filer_pb.FileChunk, invalidator CacheInvalidator) (dataChunks []*filer_pb.FileChunk, manifestResolveErr error) {
	if !chunk.IsChunkManifest {
		return
	}

	// IsChunkManifest
	bytesBuffer := bytesBufferPool.Get().(*bytes.Buffer)
	bytesBuffer.Reset()
	defer bytesBufferPool.Put(bytesBuffer)
	err := fetchWholeChunk(ctx, bytesBuffer, lookupFileIdFn, chunk.GetFileIdString(), chunk.CipherKey, chunk.IsCompressed, invalidator)
	if err != nil {
		return nil, fmt.Errorf("fail to read manifest %s: %w", chunk.GetFileIdString(), err)
	}
	m := &filer_pb.FileChunkManifest{}
	if err := proto.Unmarshal(bytesBuffer.Bytes(), m); err != nil {
		return nil, fmt.Errorf("fail to unmarshal manifest %s: %w", chunk.GetFileIdString(), err)
	}

	// recursive
	filer_pb.AfterEntryDeserialization(m.Chunks)
	return m.Chunks, nil
}

// TODO fetch from cache for weed mount?
func fetchWholeChunk(ctx context.Context, bytesBuffer *bytes.Buffer, lookupFileIdFn wdclient.LookupFileIdFunctionType, fileId string, cipherKey []byte, isGzipped bool, invalidator CacheInvalidator) error {
	urlStrings, err := lookupFileIdFn(ctx, fileId)
	if err != nil {
		glog.ErrorfCtx(ctx, "operation LookupFileId %s failed, err: %v", fileId, err)
		return err
	}
	jwt := ChunkReadJwt(urlStrings, fileId)
	if _, err = retriedStreamFetchChunkData(ctx, bytesBuffer, urlStrings, jwt, cipherKey, isGzipped, true, 0, 0, refreshUrls(ctx, invalidator, lookupFileIdFn, fileId)); err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		// a cancelled read says nothing about where the volume lives, and the
		// stream error it provoked is a symptom, not the cause
		return ctxErr
	}
	return retryFetchWithFreshLocations(ctx, invalidator, lookupFileIdFn, fileId, urlStrings, err, func(newUrls []string) error {
		// the failed attempt may have streamed a partial prefix into the buffer
		bytesBuffer.Reset()
		_, retryErr := retriedStreamFetchChunkData(ctx, bytesBuffer, newUrls, jwt, cipherKey, isGzipped, true, 0, 0, nil)
		return retryErr
	})
}

func fetchChunkRange(ctx context.Context, buffer []byte, lookupFileIdFn wdclient.LookupFileIdFunctionType, fileId string, cipherKey []byte, isGzipped bool, offset int64, refreshUrls util_http.RefreshUrlsFunc) (int, error) {
	urlStrings, err := lookupFileIdFn(ctx, fileId)
	if err != nil {
		glog.ErrorfCtx(ctx, "operation LookupFileId %s failed, err: %v", fileId, err)
		return 0, err
	}
	return util_http.RetriedFetchChunkData(ctx, buffer, urlStrings, cipherKey, isGzipped, false, offset, fileId, refreshUrls)
}

// retriedStreamFetchChunkData streams a chunk from the first location that
// answers. refreshUrls may be nil; when a location failed and a later one
// answered, it is called so the reads that follow start from a fresh list.
func retriedStreamFetchChunkData(ctx context.Context, writer io.Writer, urlStrings []string, jwt string, cipherKey []byte, isGzipped bool, isFullChunk bool, offset int64, size int, refreshUrls util_http.RefreshUrlsFunc) (written int64, err error) {

	var shouldRetry bool
	var totalWritten int

	for waitTime := time.Second; waitTime < util.RetryWaitTime; waitTime += waitTime / 2 {
		// Check for context cancellation before starting retry loop
		select {
		case <-ctx.Done():
			return int64(totalWritten), ctx.Err()
		default:
		}

		retriedCnt := 0
		var failed bool
		for _, urlString := range util_http.ReachableFirst(urlStrings) {
			// Check for context cancellation before each volume server request
			select {
			case <-ctx.Done():
				return int64(totalWritten), ctx.Err()
			default:
			}

			retriedCnt++
			var localProcessed int
			var writeErr error
			shouldRetry, err = util_http.ReadUrlAsStream(ctx, util_http.AppendQueryParameter(urlString, "readDeleted", "true"), jwt, cipherKey, isGzipped, isFullChunk, offset, size, func(data []byte) {
				// Check for context cancellation during data processing
				select {
				case <-ctx.Done():
					writeErr = ctx.Err()
					return
				default:
				}

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
				failed = true
				glog.V(0).InfofCtx(ctx, "read %s failed, err: %v", urlString, err)
			} else {
				break
			}
		}
		if err == nil && failed && refreshUrls != nil {
			refreshUrls()
		}
		// all nodes have tried it
		if retriedCnt == len(urlStrings) {
			break
		}
		if err != nil && shouldRetry {
			glog.V(0).InfofCtx(ctx, "retry reading in %v", waitTime)
			// Sleep with proper context cancellation and timer cleanup
			timer := time.NewTimer(waitTime)
			select {
			case <-ctx.Done():
				timer.Stop()
				return int64(totalWritten), ctx.Err()
			case <-timer.C:
				// Continue with retry
			}
		} else {
			break
		}
	}

	return int64(totalWritten), err

}

// MaybeManifestize folds a flat chunk list into manifest chunks once it passes
// ManifestBatch, so an entry's chunk list stays within what the metadata store
// will hold. A fold that fails partway returns inputChunks unchanged -- never
// the half-folded list, which drops the manifests the caller came in with --
// and hands the blobs it had already saved to deleteChunks, since the flat list
// it returns references none of them. deleteChunks may be nil where the caller
// has no deleter to offer; then the blobs are only named in the log.
func MaybeManifestize(saveFunc SaveDataAsChunkFunctionType, deleteChunks func([]*filer_pb.FileChunk), inputChunks []*filer_pb.FileChunk) (chunks []*filer_pb.FileChunk, err error) {
	var saved []*filer_pb.FileChunk
	record := func(reader io.Reader, name string, offset int64, tsNs int64, expectedDataSize uint64) (*filer_pb.FileChunk, error) {
		chunk, saveErr := saveFunc(reader, name, offset, tsNs, expectedDataSize)
		if saveErr == nil {
			saved = append(saved, chunk)
		}
		return chunk, saveErr
	}

	chunks, err = doMaybeManifestize(record, inputChunks, ManifestBatch, mergeIntoManifest)
	if err == nil {
		return chunks, nil
	}
	if len(saved) > 0 {
		if deleteChunks != nil {
			deleteChunks(saved)
		} else {
			glog.V(0).Infof("manifestize failed, %d manifest blobs left unreferenced: %v", len(saved), err)
		}
	}
	return inputChunks, err
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
			// dataChunks is what is left after the manifests the caller
			// already had were separated out; returning it would drop them
			return inputChunks, err
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
		return nil, fmt.Errorf("serializing manifest: %w", serErr)
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

	manifestChunk, err = saveFunc(bytes.NewReader(data), "", 0, 0, uint64(len(data)))
	if err != nil {
		return nil, err
	}
	manifestChunk.IsChunkManifest = true
	manifestChunk.Offset = minOffset
	manifestChunk.Size = uint64(maxOffset - minOffset)

	return
}

type SaveDataAsChunkFunctionType func(reader io.Reader, name string, offset int64, tsNs int64, expectedDataSize uint64) (chunk *filer_pb.FileChunk, err error)
