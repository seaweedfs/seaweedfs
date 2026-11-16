package s3api

import (
	"context"
	"errors"
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/pb"
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

// getEntry reads the upload record. Right after a ring change a part write can
// leave the new owner a bare directory before the record replicated there, so
// the prior owner's record wins over one without the upload's key, and a prior
// owner that cannot answer fails the lookup rather than reading as no upload.
func (u uploadFilerClient) getEntry(parentDirectoryPath, entryName string) (*filer_pb.Entry, error) {
	entry, err := u.s3a.getObjectScopedEntryRoutedByKey(u.bucket, u.object, parentDirectoryPath, entryName)
	if (err != nil && !errors.Is(err, filer_pb.ErrNotFound)) || isMultipartUploadEntry(entry) {
		return entry, err
	}
	prior := u.s3a.priorWriteOwner(u.bucket, u.object)
	if prior == "" {
		return entry, err
	}
	priorEntry, priorErr := u.s3a.lookupEntryOnFiler(context.Background(), prior, parentDirectoryPath, entryName)
	if priorErr != nil && !isFilerNotFound(priorErr) {
		return nil, priorErr
	}
	if priorErr == nil && isMultipartUploadEntry(priorEntry) {
		return priorEntry, nil
	}
	return entry, err
}

// list reads the owner's listing. While a ring change is in its cooling-off
// window the prior owner may still hold parts the new owner has not received,
// so its listing is merged in, the owner's entry winning a name both hold.
func (u uploadFilerClient) list(parentDirectoryPath, prefix, startFrom string, inclusive bool, limit uint32) ([]*filer_pb.Entry, bool, error) {
	entries, isLast, err := u.s3a.listObjectScopedEntriesRoutedByKey(u.bucket, u.object, parentDirectoryPath, prefix, startFrom, inclusive, limit)
	if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return entries, isLast, err
	}
	prior := u.s3a.priorWriteOwner(u.bucket, u.object)
	if prior == "" {
		return entries, isLast, err
	}
	var priorEntries []*filer_pb.Entry
	priorLast := false
	priorErr := pb.WithFilerClient(true, 0, prior, u.s3a.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		return filer_pb.SeaweedList(context.Background(), client, parentDirectoryPath, prefix, func(entry *filer_pb.Entry, isLastEntry bool) error {
			priorEntries = append(priorEntries, entry)
			priorLast = priorLast || isLastEntry
			return nil
		}, startFrom, inclusive, limit)
	})
	if priorErr != nil && !isFilerNotFound(priorErr) && len(entries) == 0 {
		// with nothing on the owner yet, every part may be on the prior owner
		return nil, false, priorErr
	}
	if priorErr != nil || len(priorEntries) == 0 {
		return entries, isLast, err
	}
	merged, mergedLast := mergeEntryListings(entries, isLast, priorEntries, priorLast, limit)
	return merged, mergedLast, nil
}

// mergeEntryListings merges two pages of one directory listed from the same
// cursor, taking primary's entry for a name both hold. Each page holds the
// first limit names past the cursor, so the merged first limit are exact.
func mergeEntryListings(primary []*filer_pb.Entry, primaryLast bool, secondary []*filer_pb.Entry, secondaryLast bool, limit uint32) ([]*filer_pb.Entry, bool) {
	byName := make(map[string]*filer_pb.Entry, len(primary)+len(secondary))
	for _, entry := range secondary {
		byName[entry.Name] = entry
	}
	for _, entry := range primary {
		byName[entry.Name] = entry
	}
	merged := make([]*filer_pb.Entry, 0, len(byName))
	for _, entry := range byName {
		merged = append(merged, entry)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Name < merged[j].Name })
	isLast := primaryLast && secondaryLast
	if limit > 0 && uint32(len(merged)) > limit {
		merged, isLast = merged[:limit], false
	}
	return merged, isLast
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
