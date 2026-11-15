package s3api

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// uploadFilerClient reads and writes one multipart upload's .uploads/<id>
// directory on the destination object's write owner, the filer the upload's
// completion commits on. Gateways on independent filer stores then see an
// upload, and its parts, as soon as the owner stored them.
type uploadFilerClient struct {
	s3a    *S3ApiServer
	bucket string
	object string
}

func (s3a *S3ApiServer) uploadFiler(bucket, object string) uploadFilerClient {
	return uploadFilerClient{s3a: s3a, bucket: bucket, object: object}
}

func (u uploadFilerClient) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	owner := u.s3a.routableWriteOwner(u.bucket, u.object)
	if owner == "" || u.s3a.filerClient == nil {
		return u.s3a.WithFilerClient(streamingMode, fn)
	}
	if u.s3a.ownerRecentlyUnreachable(owner) {
		owner = ""
	}
	return u.s3a.withFilerClientFailover(context.Background(), owner, streamingMode, true, fn)
}

func (u uploadFilerClient) AdjustedUrl(location *filer_pb.Location) string {
	return u.s3a.AdjustedUrl(location)
}

func (u uploadFilerClient) GetDataCenter() string {
	return u.s3a.GetDataCenter()
}

func (u uploadFilerClient) getEntry(parentDirectoryPath, entryName string) (*filer_pb.Entry, error) {
	return u.s3a.getObjectScopedEntryRoutedByKey(u.bucket, u.object, parentDirectoryPath, entryName)
}

func (u uploadFilerClient) list(parentDirectoryPath, prefix, startFrom string, inclusive bool, limit uint32) ([]*filer_pb.Entry, bool, error) {
	return u.s3a.listObjectScopedEntriesRoutedByKey(u.bucket, u.object, parentDirectoryPath, prefix, startFrom, inclusive, limit)
}

func (u uploadFilerClient) exists(parentDirectoryPath, entryName string, isDirectory bool) (bool, error) {
	return filer_pb.Exists(context.Background(), u, parentDirectoryPath, entryName, isDirectory)
}

func (u uploadFilerClient) mkdir(parentDirectoryPath, dirName string, fn func(entry *filer_pb.Entry)) error {
	return filer_pb.Mkdir(context.Background(), u, parentDirectoryPath, dirName, fn)
}

func (u uploadFilerClient) mkFile(parentDirectoryPath, fileName string, chunks []*filer_pb.FileChunk, fn func(entry *filer_pb.Entry)) error {
	return filer_pb.MkFile(context.Background(), u, parentDirectoryPath, fileName, chunks, fn)
}

func (u uploadFilerClient) rm(ctx context.Context, parentDirectoryPath, entryName string, isDeleteData, isRecursive bool) error {
	return retryFilerOp(ctx, "rm "+parentDirectoryPath+"/"+entryName, func() error {
		return u.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return doDeleteEntry(ctx, client, parentDirectoryPath, entryName, isDeleteData, isRecursive)
		})
	})
}
