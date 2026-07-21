package s3api

import (
	"context"
	"io"
	"math"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// saveManifestChunk returns the save function MaybeManifestize uses to store
// manifest blobs, assigning volumes against the object's real filer path so
// placement follows the bucket's storage rules.
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

// manifestizeChunks folds a large flat chunk list into manifest chunks
// (filer.ManifestBatch data chunks per manifest). On failure the flat list is
// returned so the write can still proceed, matching the filer's soft-fail.
func (s3a *S3ApiServer) manifestizeChunks(filePath string, bucket string, ttlSec int32, chunks []*filer_pb.FileChunk) []*filer_pb.FileChunk {
	manifested, err := filer.MaybeManifestize(s3a.saveManifestChunk(filePath, bucket, ttlSec), chunks)
	if err != nil {
		glog.V(0).Infof("MaybeManifestize %s: %v", filePath, err)
		return chunks
	}
	return manifested
}

// flattenManifestChunks resolves any manifest chunks on a copy source into the
// flat data-chunk list, so per-chunk copy logic reads real data chunks instead
// of raw manifest blobs.
func (s3a *S3ApiServer) flattenManifestChunks(ctx context.Context, entry *filer_pb.Entry) error {
	if entry == nil || !filer.HasChunkManifest(entry.GetChunks()) {
		return nil
	}
	dataChunks, _, err := filer.ResolveChunkManifest(ctx, s3a.createLookupFileIdFunction(), entry.GetChunks(), 0, math.MaxInt64)
	if err != nil {
		return err
	}
	entry.Chunks = dataChunks
	return nil
}
