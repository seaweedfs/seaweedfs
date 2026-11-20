package s3api

import (
	"context"
	"errors"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

// defaultUnreachableOwnerTTL is how long a route-by-key owner that just failed a
// read is skipped before being retried — long enough to spare a dead owner
// per-request dials, short enough to resume owner-first reads soon after it (or
// the ring) recovers. The unreachableOwnerTTL flag overrides it: where an owner can
// sit behind a partition that drops packets rather than refusing them, each
// retry costs a read the full routed-lookup deadline, so a longer skip helps.
const defaultUnreachableOwnerTTL = 2 * time.Second

func (s3a *S3ApiServer) unreachableOwnerTTL() time.Duration {
	if s3a.option != nil && s3a.option.UnreachableOwnerTTL > 0 {
		return s3a.option.UnreachableOwnerTTL
	}
	return defaultUnreachableOwnerTTL
}

// routedLookupContext returns the context for one attempt of a route-by-key
// lookup's failover walk. The walk tries preferred first; when that owner is
// another filer than the current one, its attempt gets the RoutedLookupTimeout
// deadline, so an owner that cannot answer (a dropped SYN waits out gRPC's 20 s
// connect timeout) costs the read that long before the walk asks the next
// filer, any of which reads the same entry. The current filer and every later
// attempt run unbounded, as before. The deadline covers the wait for the
// owner's first response only: answered lifts it, so a stream that has started
// answering runs to its end (the walk cannot replay it past that point).
//
// The returned function must be called once per attempt, in walk order.
func (s3a *S3ApiServer) routedLookupContext(preferred pb.ServerAddress) func() (ctx context.Context, answered func(), cancel context.CancelFunc) {
	bounded := preferred != "" && s3a.option != nil && s3a.option.RoutedLookupTimeout > 0 &&
		preferred != s3a.filerClient.GetCurrentFiler()
	return func() (context.Context, func(), context.CancelFunc) {
		if !bounded {
			return context.Background(), func() {}, func() {}
		}
		bounded = false
		ctx, cancel := context.WithCancelCause(context.Background())
		timer := time.AfterFunc(s3a.option.RoutedLookupTimeout, func() { cancel(context.DeadlineExceeded) })
		return ctx, func() { timer.Stop() }, func() { timer.Stop(); cancel(nil) }
	}
}

// routedProbeContext bounds a single-filer probe of a key's prior owner the way
// routedLookupContext bounds the walk's first attempt.
func (s3a *S3ApiServer) routedProbeContext(filer pb.ServerAddress) (context.Context, context.CancelFunc) {
	ctx, _, cancel := s3a.routedLookupContext(filer)()
	return ctx, cancel
}

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
	attemptContext := s3a.routedLookupContext(preferred)
	err := s3a.withFilerClientFailover(context.Background(), preferred, false, true, func(client filer_pb.SeaweedFilerClient) error {
		ctx, _, cancel := attemptContext()
		defer cancel()
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

	// A just-moved key may not have replicated to the new owner yet; consult its
	// prior owner once while the ring change is within the cooling-off window.
	if errors.Is(err, filer_pb.ErrNotFound) {
		if prior := s3a.priorWriteOwner(bucket, object); prior != "" && prior != owner {
			probeCtx, cancel := s3a.routedProbeContext(prior)
			priorEntry, priorErr := s3a.lookupEntryOnFiler(probeCtx, prior, dir, name)
			cancel()
			if priorErr == nil {
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
		attemptContext := s3a.routedLookupContext(preferred)
		return s3a.withFilerClientFailover(context.Background(), preferred, false, true, func(client filer_pb.SeaweedFilerClient) error {
			ctx, answered, cancel := attemptContext()
			defer cancel()
			return filer_pb.SeaweedList(ctx, answeringClient{client, answered}, parentDirectoryPath, prefix, func(entry *filer_pb.Entry, isLastEntry bool) error {
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

// answeringClient calls answered once a listing stream delivers its first
// message, ahead of SeaweedList's one-entry look-ahead.
type answeringClient struct {
	filer_pb.SeaweedFilerClient
	answered func()
}

func (c answeringClient) ListEntries(ctx context.Context, in *filer_pb.ListEntriesRequest, opts ...grpc.CallOption) (filer_pb.SeaweedFiler_ListEntriesClient, error) {
	stream, err := c.SeaweedFilerClient.ListEntries(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return answeringStream{stream, c.answered}, nil
}

type answeringStream struct {
	filer_pb.SeaweedFiler_ListEntriesClient
	answered func()
}

func (s answeringStream) Recv() (*filer_pb.ListEntriesResponse, error) {
	resp, err := s.SeaweedFiler_ListEntriesClient.Recv()
	if err == nil {
		s.answered()
	}
	return resp, err
}

func (s3a *S3ApiServer) priorWriteOwner(bucket, object string) pb.ServerAddress {
	if object == "" || s3a.objectWriteLockClient == nil {
		return ""
	}
	return s3a.objectWriteLockClient.PriorOwnerForKey(s3a.objectRouteKey(bucket, object))
}

func (s3a *S3ApiServer) markOwnerUnreachable(owner pb.ServerAddress) {
	s3a.unreachableOwners.Store(owner, time.Now().Add(s3a.unreachableOwnerTTL()))
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
