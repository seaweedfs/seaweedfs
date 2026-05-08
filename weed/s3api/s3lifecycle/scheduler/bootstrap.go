package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/dispatcher"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/engine"
)

// BucketBootstrapper backfills already-existing entries when a freshly-PUT
// rule's bucket appears in the engine. The reader-driven path only sees
// events created after the rule lands; without this walk, objects PUT
// before the rule would never expire.
//
// Per bucket: one looping goroutine that re-walks every WalkInterval until
// ctx is canceled. The walk only dispatches currently-due actions, so a
// 1-second-old entry under a 10-second rule sleeps in place and gets
// picked up by a later iteration. Each LifecycleDelete that succeeds
// removes the entry, so a stable bucket converges.
//
// Snapshot is fetched fresh from GetSnapshot every walk, so a rule update
// that disables an action stops dispatching it on the next iteration
// without restarting the goroutine.
type BucketBootstrapper struct {
	FilerClient  filer_pb.SeaweedFilerClient
	Client       dispatcher.LifecycleClient
	BucketsPath  string
	WalkInterval time.Duration
	GetSnapshot  func() *engine.Snapshot

	mu    sync.Mutex
	known map[string]bool
}

// KickOffNew launches a per-bucket walker goroutine for every bucket
// in `buckets` that hasn't been seen before. Safe to call from many
// callers concurrently; only the first call per bucket spawns work.
func (b *BucketBootstrapper) KickOffNew(ctx context.Context, buckets []string) {
	b.mu.Lock()
	if b.known == nil {
		b.known = map[string]bool{}
	}
	fresh := make([]string, 0, len(buckets))
	for _, bucket := range buckets {
		if b.known[bucket] {
			continue
		}
		b.known[bucket] = true
		fresh = append(fresh, bucket)
	}
	b.mu.Unlock()

	for _, bucket := range fresh {
		bucket := bucket
		go b.runBucket(ctx, bucket)
	}
}

func (b *BucketBootstrapper) runBucket(ctx context.Context, bucket string) {
	interval := b.WalkInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	disp := &bootstrapDispatcher{client: b.Client, bucket: bucket}
	walk := func() {
		snap := b.GetSnapshot()
		if snap == nil {
			return
		}
		now := time.Now().UTC()
		if _, err := bootstrap.Walk(ctx, snap, bucket, b.listFunc(), disp, bootstrap.WalkOptions{Now: now}); err != nil {
			if ctx.Err() == nil {
				glog.V(1).Infof("lifecycle bootstrap %s: %v", bucket, err)
			}
		}
	}
	// Run once immediately so currently-due entries fire without waiting for
	// the next tick, then loop until shutdown.
	walk()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			walk()
		}
	}
}

func (b *BucketBootstrapper) listFunc() bootstrap.ListFunc {
	return func(ctx context.Context, bucket, start string, cb func(*bootstrap.Entry) error) error {
		root := strings.TrimSuffix(b.BucketsPath, "/") + "/" + bucket
		return walkBucketDir(ctx, b.FilerClient, root, root, cb)
	}
}

func walkBucketDir(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, bucketRoot string, cb func(*bootstrap.Entry) error) error {
	var children []*filer_pb.Entry
	if err := filer_pb.SeaweedList(ctx, client, dir, "", func(e *filer_pb.Entry, _ bool) error {
		children = append(children, e)
		return nil
	}, "", false, 0); err != nil {
		return fmt.Errorf("list %s: %w", dir, err)
	}
	for _, entry := range children {
		if entry == nil || entry.Attributes == nil {
			continue
		}
		full := dir + "/" + entry.Name
		key := strings.TrimPrefix(full, bucketRoot+"/")

		if entry.IsDirectory {
			if dest, ok := mpuInitDestKey(key, entry); ok {
				if err := cb(&bootstrap.Entry{
					Path:        key,
					DestKey:     dest,
					ModTime:     entryMtime(entry),
					IsDirectory: true,
					IsMPUInit:   true,
				}); err != nil {
					return err
				}
				continue
			}
			if err := walkBucketDir(ctx, client, full, bucketRoot, cb); err != nil {
				return err
			}
			continue
		}

		if err := cb(&bootstrap.Entry{
			Path:        key,
			ModTime:     entryMtime(entry),
			Size:        int64(entry.Attributes.FileSize),
			IsLatest:    true,
			NumVersions: 1,
			Tags:        extractObjectTags(entry.Extended),
		}); err != nil {
			return err
		}
	}
	return nil
}

// mpuInitDestKey mirrors router.mpuInitInfo: a directory at .uploads/<id>
// carrying the destination key in Extended is the MPU init record. The
// router helper is package-private; reproduced here so bootstrap doesn't
// reach into router internals.
func mpuInitDestKey(key string, entry *filer_pb.Entry) (string, bool) {
	uploadsPrefix := s3_constants.MultipartUploadsFolder + "/"
	if !strings.HasPrefix(key, uploadsPrefix) {
		return "", false
	}
	rest := key[len(uploadsPrefix):]
	if rest == "" || strings.ContainsRune(rest, '/') {
		return "", false
	}
	v, ok := entry.Extended[s3_constants.ExtMultipartObjectKey]
	if !ok || len(v) == 0 {
		return "", false
	}
	return string(v), true
}

func extractObjectTags(ext map[string][]byte) map[string]string {
	if len(ext) == 0 {
		return nil
	}
	prefix := s3_constants.AmzObjectTagging + "-"
	var out map[string]string
	for k, v := range ext {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[k[len(prefix):]] = string(v)
	}
	return out
}

func entryMtime(entry *filer_pb.Entry) time.Time {
	if entry.Attributes == nil {
		return time.Time{}
	}
	return time.Unix(entry.Attributes.Mtime, int64(entry.Attributes.MtimeNs))
}

// bootstrapDispatcher implements bootstrap.Dispatcher by issuing a
// LifecycleDelete RPC with no expected_identity (the server treats nil
// as "skip CAS"). Bootstrap entries don't carry the chunk fid / extended
// hash the live event path captures, so a CAS witness here would just
// noop-resolve the request.
type bootstrapDispatcher struct {
	client dispatcher.LifecycleClient
	bucket string
}

func (d *bootstrapDispatcher) Delete(ctx context.Context, action *engine.CompiledAction, entry *bootstrap.Entry) error {
	rh := action.Key.RuleHash
	resp, err := d.client.LifecycleDelete(ctx, &s3_lifecycle_pb.LifecycleDeleteRequest{
		Bucket:     d.bucket,
		ObjectPath: entry.Path,
		RuleHash:   rh[:],
		ActionKind: bootstrapToProtoActionKind(action.Key.ActionKind),
	})
	if err != nil {
		return fmt.Errorf("rpc: %w", err)
	}
	switch resp.Outcome {
	case s3_lifecycle_pb.LifecycleDeleteOutcome_DONE,
		s3_lifecycle_pb.LifecycleDeleteOutcome_NOOP_RESOLVED,
		s3_lifecycle_pb.LifecycleDeleteOutcome_SKIPPED_OBJECT_LOCK:
		return nil
	default:
		// Don't halt the walk on a single RETRY_LATER / BLOCKED — the
		// reader-driven path picks the same entry up on its next event,
		// or the next bootstrap iteration will retry.
		glog.V(1).Infof("lifecycle bootstrap %s/%s: outcome=%s reason=%s",
			d.bucket, entry.Path, resp.Outcome, resp.Reason)
		return nil
	}
}

func bootstrapToProtoActionKind(k s3lifecycle.ActionKind) s3_lifecycle_pb.ActionKind {
	switch k {
	case s3lifecycle.ActionKindExpirationDays:
		return s3_lifecycle_pb.ActionKind_EXPIRATION_DAYS
	case s3lifecycle.ActionKindExpirationDate:
		return s3_lifecycle_pb.ActionKind_EXPIRATION_DATE
	case s3lifecycle.ActionKindNoncurrentDays:
		return s3_lifecycle_pb.ActionKind_NONCURRENT_DAYS
	case s3lifecycle.ActionKindNewerNoncurrent:
		return s3_lifecycle_pb.ActionKind_NEWER_NONCURRENT
	case s3lifecycle.ActionKindAbortMPU:
		return s3_lifecycle_pb.ActionKind_ABORT_MPU
	case s3lifecycle.ActionKindExpiredDeleteMarker:
		return s3_lifecycle_pb.ActionKind_EXPIRED_DELETE_MARKER
	}
	return s3_lifecycle_pb.ActionKind_ACTION_KIND_UNSPECIFIED
}
