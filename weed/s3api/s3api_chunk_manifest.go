package s3api

import (
	"context"
	"io"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// saveManifestChunk stores manifest blobs, assigning volumes against the
// object's filer path so bucket placement rules apply.
func (s3a *S3ApiServer) saveManifestChunk(filePath string, bucket string, ttlSec int32) filer.SaveDataAsChunkFunctionType {
	collection := ""
	if s3a.option.FilerGroup != "" {
		collection = s3a.getCollectionName(bucket)
	}
	return func(reader io.Reader, name string, offset int64, tsNs int64, expectedDataSize uint64) (*filer_pb.FileChunk, error) {
		return filer.SaveGatewayDataAsChunk(filer.GatewayChunkUploadRequest{
			FilerClient: s3a,
			Reader:      reader,
			FullPath:    filePath,
			Offset:      offset,
			TsNs:        tsNs,
			Collection:  collection,
			TtlSec:      ttlSec,
			DataCenter:  s3a.option.DataCenter,
			Cipher:      s3a.cipher,
		})
	}
}

// manifestizeChunks folds a large flat chunk list into manifest chunks. A
// failed fold falls back to the flat list, deleting any blobs it saved.
func (s3a *S3ApiServer) manifestizeChunks(filePath string, bucket string, ttlSec int32, chunks []*filer_pb.FileChunk) []*filer_pb.FileChunk {
	return manifestizeOrKeepFlat(s3a.saveManifestChunk(filePath, bucket, ttlSec), s3a.deleteOrphanedChunks, filePath, chunks)
}

func manifestizeOrKeepFlat(save filer.SaveDataAsChunkFunctionType, deleteChunks func([]*filer_pb.FileChunk), filePath string, chunks []*filer_pb.FileChunk) []*filer_pb.FileChunk {
	var saved []*filer_pb.FileChunk
	record := func(reader io.Reader, name string, offset int64, tsNs int64, expectedDataSize uint64) (*filer_pb.FileChunk, error) {
		chunk, err := save(reader, name, offset, tsNs, expectedDataSize)
		if err == nil {
			saved = append(saved, chunk)
		}
		return chunk, err
	}
	manifested, err := filer.MaybeManifestize(record, chunks)
	if err != nil {
		glog.V(0).Infof("MaybeManifestize %s: %v", filePath, err)
		if len(saved) > 0 {
			deleteChunks(saved)
		}
		return chunks
	}
	return manifested
}

// flattenManifestChunks resolves manifest chunks into flat data chunks,
// returning the resolved-away manifests for callers that must delete the
// superseded blobs once they own the data chunks.
func (s3a *S3ApiServer) flattenManifestChunks(ctx context.Context, entry *filer_pb.Entry) ([]*filer_pb.FileChunk, error) {
	if entry == nil || !filer.HasChunkManifest(entry.GetChunks()) {
		return nil, nil
	}
	dataChunks, manifestChunks, err := filer.ResolveChunkManifest(ctx, s3a.createLookupFileIdFunction(), entry.GetChunks(), 0, math.MaxInt64)
	if err != nil {
		return nil, err
	}
	entry.Chunks = dataChunks
	return manifestChunks, nil
}
