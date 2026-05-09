package scheduler

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

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
	var cb func(entry *filer_pb.Entry, key string) error
	cb = func(entry *filer_pb.Entry, key string) error {
		// .versions/-suffixed directories may be SeaweedFS version
		// storage OR a coincidentally-named user folder.
		// expandVersionsDir disambiguates by inspecting children and
		// falls back to a regular recursion via cb when the dir holds
		// no version files.
		if isVersionsDir(entry) {
			n, err := b.expandVersionsDir(ctx, bucket, root, key, entry, cb)
			count += n
			return err
		}
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
	}
	if err := walkBucketDir(ctx, b.FilerClient, root, root, cb); err != nil {
		if ctx.Err() == nil {
			glog.V(0).Infof("lifecycle bootstrap %s: %v", bucket, err)
		}
		return
	}
	glog.V(0).Infof("lifecycle bootstrap: bucket %s injected %d entries", bucket, count)
}

// expandVersionsDir lists <root>/<key>/ and, when the children look like
// SeaweedFS version files (each carries ExtVersionIdKey), injects one
// reader.Event per version with BootstrapVersion populated. versionsKey
// is the bucket-relative path of the directory itself (e.g.
// "logs/foo.versions"). SuccessorModTime is the immediately newer
// sibling's mtime — when this version became noncurrent, the clock
// NoncurrentDays uses.
//
// When no child has ExtVersionIdKey the directory is treated as a
// coincidentally-named user folder and walked recursively via fallback,
// which is the same callback walkBucket builds. Reusing the same cb
// keeps any nested .versions/<x> within it routed correctly.
func (b *BucketBootstrapper) expandVersionsDir(ctx context.Context, bucket, root, versionsKey string, versionsEntry *filer_pb.Entry, fallback func(*filer_pb.Entry, string) error) (int, error) {
	logical := strings.TrimSuffix(versionsKey, s3_constants.VersionsFolder)
	if logical == "" {
		return 0, nil
	}
	versionsDir := strings.TrimSuffix(b.BucketsPath, "/") + "/" + bucket + "/" + versionsKey
	// First pass: collect every direct child that's a file (subdirs would
	// corrupt the mtime sort and rank math). The hasVersionFile check
	// inspects this raw set so a .versions/-suffixed user folder full of
	// directories falls through to the recursion below.
	var children []*filer_pb.Entry
	if err := filer_pb.SeaweedList(ctx, b.FilerClient, versionsDir, "", func(e *filer_pb.Entry, _ bool) error {
		if e != nil && e.Attributes != nil && !e.IsDirectory {
			children = append(children, e)
		}
		return nil
	}, "", false, 0); err != nil {
		return 0, fmt.Errorf("list %s: %w", versionsDir, err)
	}
	// Disambiguate: a real .versions container's children always carry
	// ExtVersionIdKey. createDeleteMarker writes the version file with
	// the key set, so even mid-update the children look like versions.
	versions := make([]*filer_pb.Entry, 0, len(children))
	for _, e := range children {
		if id, ok := e.Extended[s3_constants.ExtVersionIdKey]; ok && len(id) > 0 {
			versions = append(versions, e)
		}
	}
	if len(versions) == 0 {
		// Coincidentally-named user folder (or an empty .versions
		// container). fallback is the bucket walk's own cb so nested
		// .versions/ entries inside still expand.
		if fallback == nil {
			return 0, nil
		}
		if err := walkBucketDir(ctx, b.FilerClient, versionsDir, root, fallback); err != nil {
			return 0, err
		}
		return 0, nil
	}
	// Newest-first by mtime: NoncurrentIndex is 0-based among noncurrents
	// in that order, and SuccessorModTime is the next-newer sibling's mtime.
	sort.SliceStable(versions, func(i, j int) bool {
		mi := versions[i].Attributes.Mtime*int64(1e9) + int64(versions[i].Attributes.MtimeNs)
		mj := versions[j].Attributes.Mtime*int64(1e9) + int64(versions[j].Attributes.MtimeNs)
		return mi > mj
	})
	// Resolve the latest position. If the directory's latest pointer is
	// missing or names a version that's no longer present (rare, e.g.
	// race with createDeleteMarker), fall back to the newest sibling
	// rather than mark every version noncurrent.
	latestID := string(versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey])
	latestPos := 0
	if latestID != "" {
		for i, v := range versions {
			if string(v.Extended[s3_constants.ExtVersionIdKey]) == latestID {
				latestPos = i
				break
			}
		}
	}
	count := 0
	for i, v := range versions {
		versionID := string(v.Extended[s3_constants.ExtVersionIdKey])
		if versionID == "" {
			continue
		}
		var successor time.Time
		if i > 0 {
			successor = time.Unix(versions[i-1].Attributes.Mtime, int64(versions[i-1].Attributes.MtimeNs))
		}
		bv := &reader.BootstrapVersion{
			LogicalKey:       logical,
			VersionID:        versionID,
			IsLatest:         i == latestPos,
			IsDeleteMarker:   string(v.Extended[s3_constants.ExtDeleteMarkerKey]) == "true",
			NumVersions:      len(versions),
			SuccessorModTime: successor,
		}
		if !bv.IsLatest {
			// 0-based among noncurrents in newest-first order.
			rank := i
			if i > latestPos {
				rank = i - 1
			}
			bv.NoncurrentIndex = rank
		}
		ev := &reader.Event{
			TsNs:             0,
			Bucket:           bucket,
			Key:              versionsKey + "/" + v.Name,
			ShardID:          s3lifecycle.ShardID(bucket, logical),
			NewEntry:         v,
			BootstrapVersion: bv,
		}
		if err := b.Injector.InjectEvent(ctx, ev); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// walkBucketDir lists every file under dir recursively and invokes cb
// with the filer entry plus its bucket-relative key. Two kinds of
// directories are emitted whole rather than recursed into:
//   - .uploads/<id> MPU init dirs (router fires ABORT_MPU off the dir entry)
//   - <key>.versions/ directories (caller expands them into per-version
//     events; recursing here would emit individual version files without
//     the sibling state needed for NoncurrentDays / NewerNoncurrent)
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
			if isVersionsDir(entry) {
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

// isVersionsDir matches `<x>.versions/` by name suffix. We can't gate on
// ExtLatestVersionIdKey here: createDeleteMarker writes the version file
// before updating the parent's Extended pointer, so a walk that races
// with that update would see the directory without the pointer and
// recurse into raw version files, losing the sibling state needed for
// noncurrent rules. expandVersionsDir handles disambiguation by
// inspecting children for ExtVersionIdKey; coincidentally-named
// directories that aren't real .versions storage fall through to a
// regular recursion.
func isVersionsDir(entry *filer_pb.Entry) bool {
	return entry.IsDirectory && strings.HasSuffix(entry.Name, s3_constants.VersionsFolder)
}

