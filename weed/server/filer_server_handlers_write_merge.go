package weed_server

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"io"
	"math"
)

const MergeChunkMinCount int = 1000

func (fs *FilerServer) maybeMergeChunks(ctx context.Context, so *operation.StorageOption, inputChunks []*filer_pb.FileChunk) (mergedChunks []*filer_pb.FileChunk, err error) {
	// Only merge small chunks more than half of the file
	var chunkSize = fs.option.MaxMB * 1024 * 1024
	var smallChunk, sumChunk int
	var minOffset int64 = math.MaxInt64
	for _, chunk := range inputChunks {
		if chunk.IsChunkManifest {
			continue
		}
		if chunk.Size < uint64(chunkSize/2) {
			smallChunk++
			if chunk.Offset < minOffset {
				minOffset = chunk.Offset
			}
		}
		sumChunk++
	}
	if smallChunk < MergeChunkMinCount || smallChunk < sumChunk/2 {
		return inputChunks, nil
	}

	return fs.mergeChunks(ctx, so, inputChunks, minOffset)
}

func (fs *FilerServer) mergeChunks(ctx context.Context, so *operation.StorageOption, inputChunks []*filer_pb.FileChunk, chunkOffset int64) (mergedChunks []*filer_pb.FileChunk, mergeErr error) {
	chunkedFileReader := filer.NewChunkStreamReaderFromFiler(ctx, fs.filer.MasterClient, inputChunks)
	_, mergeErr = chunkedFileReader.Seek(chunkOffset, io.SeekCurrent)
	if mergeErr != nil {
		return nil, mergeErr
	}
	mergedChunks, _, _, mergeErr, _ = fs.uploadReaderToChunks(ctx, chunkedFileReader, chunkOffset, int32(fs.option.MaxMB*1024*1024), "", "", true, so)
	if mergeErr != nil {
		return
	}

	stats.FilerHandlerCounter.WithLabelValues(stats.ChunkMerge).Inc()
	for _, chunk := range inputChunks {
		if chunk.Offset < chunkOffset || chunk.IsChunkManifest {
			mergedChunks = append(mergedChunks, chunk)
		}
	}

	garbage, err := filer.MinusChunks(ctx, fs.lookupFileId, inputChunks, mergedChunks)
	if err != nil {
		glog.Errorf("Failed to resolve old entry chunks when delete old entry chunks. new: %s, old: %s",
			mergedChunks, inputChunks)
		return mergedChunks, err
	}
	fs.filer.DeleteChunksNotRecursive(garbage)
	return
}
