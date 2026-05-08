package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/reader"
)

// EventInjector is the bootstrap-side hook into the dispatcher pipeline.
// One implementation routes events to the right per-shard pipeline; the
// shell's single-pipeline path passes pipeline.InjectEvent directly.
type EventInjector interface {
	InjectEvent(ctx context.Context, ev *reader.Event) error
}

// BucketBootstrapper backfills already-existing entries when a freshly-PUT
// rule's bucket appears in the engine. The reader-driven path only sees
// meta-log events created after the rule lands; without this walk,
// objects PUT before the rule would never expire.
//
// Per bucket: one one-shot goroutine that lists every entry under
// /buckets/<bucket> and synthesizes a *reader.Event for each one. The
// pipeline's existing router.Route + Schedule machinery handles the rest:
// currently-due matches fire on the next dispatch tick, and not-yet-due
// matches sit in the per-shard schedule until their DueTime arrives.
//
// Synthesized events carry TsNs=0 so dispatcher.advance is a no-op for
// them — the reader still resumes from its persisted cursor on restart.
//
// Bucket completion is in-memory per process; a fresh worker run walks
// each bucket once on first refresh.
type BucketBootstrapper struct {
	FilerClient filer_pb.SeaweedFilerClient
	BucketsPath string
	Injector    EventInjector

	mu    sync.Mutex
	known map[string]bool
}

// KickOffNew launches a one-shot walker goroutine for every bucket
// in `buckets` that hasn't been seen before.
func (b *BucketBootstrapper) KickOffNew(ctx context.Context, buckets []string) {
	if b.Injector == nil {
		return
	}
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
		go b.walkBucket(ctx, bucket)
	}
}

func (b *BucketBootstrapper) walkBucket(ctx context.Context, bucket string) {
	root := strings.TrimSuffix(b.BucketsPath, "/") + "/" + bucket
	glog.V(0).Infof("lifecycle bootstrap: starting walk for bucket %s (root=%s)", bucket, root)
	count := 0
	if err := walkBucketDir(ctx, b.FilerClient, root, root, func(entry *filer_pb.Entry, key string) error {
		ev := &reader.Event{
			// TsNs=0 sentinel: dispatcher.advance treats <=0 as no-op,
			// so the reader's persisted cursor isn't ratcheted forward
			// past meta-log events that haven't been processed yet.
			TsNs:     0,
			Bucket:   bucket,
			Key:      key,
			ShardID:  s3lifecycle.ShardID(bucket, key),
			NewEntry: entry,
		}
		count++
		return b.Injector.InjectEvent(ctx, ev)
	}); err != nil {
		if ctx.Err() == nil {
			glog.V(0).Infof("lifecycle bootstrap %s: %v", bucket, err)
		}
		return
	}
	glog.V(0).Infof("lifecycle bootstrap: bucket %s injected %d entries", bucket, count)
}

// walkBucketDir lists every file under dir recursively and invokes cb
// with the filer entry plus its bucket-relative key. MPU init dirs at
// .uploads/<id> are emitted as a single (directory-shaped) entry so the
// router's MPU detection fires; deeper directories recurse.
func walkBucketDir(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, bucketRoot string, cb func(entry *filer_pb.Entry, key string) error) error {
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
			if isMPUInitDir(key, entry) {
				if err := cb(entry, key); err != nil {
					return err
				}
				continue
			}
			if err := walkBucketDir(ctx, client, full, bucketRoot, cb); err != nil {
				return err
			}
			continue
		}
		if err := cb(entry, key); err != nil {
			return err
		}
	}
	return nil
}

// isMPUInitDir mirrors router.mpuInitInfo: a directory at .uploads/<id>
// carrying the destination key in Extended is the MPU init record. The
// router helper is package-private so this is duplicated rather than
// adding a public extraction API just for this caller.
func isMPUInitDir(key string, entry *filer_pb.Entry) bool {
	uploadsPrefix := s3_constants.MultipartUploadsFolder + "/"
	if !strings.HasPrefix(key, uploadsPrefix) {
		return false
	}
	rest := key[len(uploadsPrefix):]
	if rest == "" || strings.ContainsRune(rest, '/') {
		return false
	}
	v, ok := entry.Extended[s3_constants.ExtMultipartObjectKey]
	return ok && len(v) > 0
}

