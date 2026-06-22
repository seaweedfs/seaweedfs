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
	fullPath := util.NewFullPath(s3a.bucketDir(bucket), object)

	owner := s3a.routableWriteOwner(bucket, object)
	if owner == "" || s3a.filerClient == nil {
		return filer_pb.GetEntry(context.Background(), s3a, fullPath)
	}

	// Skip an owner whose recent read hit a transport error; read local-first until
	// it (or the ring) recovers, rather than re-dialing a dead owner every request.
	preferred := owner
	if s3a.ownerRecentlyUnreachable(owner) {
		preferred = ""
	}

	dir, name := fullPath.DirAndName()
	var entry *filer_pb.Entry
	err := s3a.withFilerClientFailover(preferred, false, func(client filer_pb.SeaweedFilerClient) error {
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
			if priorEntry, priorErr := s3a.lookupEntryOnFiler(prior, dir, name); priorErr == nil {
				return priorEntry, nil
			}
		}
	}
	return entry, err
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

// lookupEntryOnFiler resolves dir/name against a single filer, without failover.
func (s3a *S3ApiServer) lookupEntryOnFiler(filer pb.ServerAddress, dir, name string) (*filer_pb.Entry, error) {
	var entry *filer_pb.Entry
	err := pb.WithFilerClient(false, 0, filer, s3a.option.GrpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
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
	return entry, err
}
