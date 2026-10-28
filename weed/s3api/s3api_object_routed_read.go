package s3api

import (
	"context"
	"errors"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// unreachableOwnerTTL is how long a route-by-key owner that just failed a read is
// skipped before being retried — long enough to spare a dead owner per-request
// dials, short enough to resume owner-first reads soon after it (or the ring) recovers.
const unreachableOwnerTTL = 2 * time.Second

// getObjectEntryRoutedByKey resolves an object's entry preferring the key's write
// owner (the same route key the write path hashes), so a read sees a just-written
// object without waiting for cross-filer replication. On the owner's ErrNotFound it
// probes the key's prior owner once during a rebalance window; falls back to
// getEntry when no owner is resolvable.
func (s3a *S3ApiServer) getObjectEntryRoutedByKey(bucket, object string) (*filer_pb.Entry, error) {
	dir, name := util.NewFullPath(s3a.bucketDir(bucket), object).DirAndName()
	return s3a.getObjectScopedEntryRoutedByKey(bucket, object, dir, name)
}

// getObjectScopedEntryRoutedByKey reads dir/name the way
// getObjectEntryRoutedByKey reads the object itself, for the entries an
// object's writes commit on its owner: its .versions directory and the version
// files under it. bucket/object picks the owner; dir/name picks the entry.
func (s3a *S3ApiServer) getObjectScopedEntryRoutedByKey(bucket, object, dir, name string) (*filer_pb.Entry, error) {
	owner := s3a.routableWriteOwner(bucket, object)
	if owner == "" || s3a.filerClient == nil {
		return s3a.getEntry(dir, name)
	}

	// Skip an owner whose recent read hit a transport error; read local-first until
	// it (or the ring) recovers, rather than re-dialing a dead owner every request.
	preferred := owner
	if s3a.ownerRecentlyUnreachable(owner) {
		preferred = ""
	}

	var entry *filer_pb.Entry
	err := s3a.withFilerClientFailover(context.Background(), preferred, false, true, func(client filer_pb.SeaweedFilerClient) error {
		resp, lookupErr := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if lookupErr != nil {
			return lookupErr
		}
		entry = resp.Entry
		return nil
	})

	// A just-moved key may not have replicated to the new owner yet; consult its
	// prior owner once while the ring change is within the cooling-off window.
	if errors.Is(err, filer_pb.ErrNotFound) {
		if prior := s3a.priorWriteOwner(bucket, object); prior != "" && prior != owner {
			if priorEntry, priorErr := s3a.lookupEntryOnFiler(context.Background(), prior, dir, name); priorErr == nil {
				return priorEntry, nil
			}
		}
	}
	return entry, err
}

// listObjectScopedEntriesRoutedByKey lists parentDirectoryPath preferring
// bucket/object's write owner, for scans of directories that commit under the
// object's owner (.versions). Mirrors s3a.list when no owner is resolvable.
func (s3a *S3ApiServer) listObjectScopedEntriesRoutedByKey(bucket, object, parentDirectoryPath, prefix, startFrom string, inclusive bool, limit uint32) (entries []*filer_pb.Entry, isLast bool, err error) {
	owner := s3a.routableWriteOwner(bucket, object)
	if owner == "" || s3a.filerClient == nil {
		return s3a.list(parentDirectoryPath, prefix, startFrom, inclusive, limit)
	}
	preferred := owner
	if s3a.ownerRecentlyUnreachable(owner) {
		preferred = ""
	}
	err = listRetry(parentDirectoryPath, func() error {
		entries = entries[:0]
		isLast = false
		return s3a.withFilerClientFailover(context.Background(), preferred, false, true, func(client filer_pb.SeaweedFilerClient) error {
			return filer_pb.SeaweedList(context.Background(), client, parentDirectoryPath, prefix, func(entry *filer_pb.Entry, isLastEntry bool) error {
				entries = append(entries, entry)
				if isLastEntry {
					isLast = true
				}
				return nil
			}, startFrom, inclusive, limit)
		})
	})
	if len(entries) == 0 {
		isLast = true
	}
	return entries, isLast, err
}

func (s3a *S3ApiServer) priorWriteOwner(bucket, object string) pb.ServerAddress {
	if object == "" || s3a.objectWriteLockClient == nil {
		return ""
	}
	return s3a.objectWriteLockClient.PriorOwnerForKey(s3a.objectRouteKey(bucket, object))
}

func (s3a *S3ApiServer) markOwnerUnreachable(owner pb.ServerAddress) {
	s3a.unreachableOwners.Store(owner, time.Now().Add(unreachableOwnerTTL))
}

func (s3a *S3ApiServer) ownerRecentlyUnreachable(owner pb.ServerAddress) bool {
	if v, ok := s3a.unreachableOwners.Load(owner); ok {
		return time.Now().Before(v.(time.Time))
	}
	return false
}

// lookupEntryPreferringOwner reads an entry back from a known write owner, so a
// caller that just wrote there sees its own write. Unlike getObjectEntryRoutedByKey
// it never drops the owner for a healthier peer, which would read behind the write.
func (s3a *S3ApiServer) lookupEntryPreferringOwner(owner pb.ServerAddress, dir, name string) (*filer_pb.Entry, error) {
	if owner == "" {
		return s3a.getEntry(dir, name)
	}
	return s3a.lookupEntryOnFiler(context.Background(), owner, dir, name)
}

// lookupEntryOnFiler resolves dir/name against a single filer, without failover.
func (s3a *S3ApiServer) lookupEntryOnFiler(ctx context.Context, filer pb.ServerAddress, dir, name string) (*filer_pb.Entry, error) {
	var entry *filer_pb.Entry
	err := pb.WithFilerClient(false, 0, filer, s3a.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, lookupErr := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if lookupErr != nil {
			return lookupErr
		}
		entry = resp.Entry
		return nil
	})
	return entry, err
}
