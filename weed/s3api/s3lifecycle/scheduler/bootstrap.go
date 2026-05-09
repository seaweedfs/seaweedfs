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
	"github.com/seaweedfs/seaweedfs/weed/util"
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
	// skipBare records bucket-relative bare-key paths that
	// expandVersionsDir already routed as the null version. Without it
	// the walker's regular emission would also fire for the bare entry
	// — in a versioned bucket buildObjectInfo classifies it as
	// IsLatest=true, NumVersions=0, and ExpirationDays would create a
	// stray delete marker that hides the real latest.
	skipBare := map[string]bool{}
	var cb func(entry *filer_pb.Entry, key string) error
	cb = func(entry *filer_pb.Entry, key string) error {
		if isVersionsDir(entry) {
			n, err := b.expandVersionsDir(ctx, bucket, root, key, entry, cb, skipBare)
			count += n
			return err
		}
		if !entry.IsDirectory && skipBare[key] {
			return nil
		}
		if entry.IsDirectoryKeyObject() && skipBare[key] {
			return nil
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

// versionItem is the per-sibling state expandVersionsDir builds: the
// filer entry plus its version_id (or "null" for the bare null version
// living outside .versions/). isExplicitNull marks bare entries the
// suspended-versioning write path tagged with ExtVersionIdKey="null"
// (s3api_object_handlers_put.go); we trust those as latest when the
// .versions/ pointer is missing. A pre-versioning bare object has no
// such marker, so a missing pointer there is a race window with a new
// version write and we keep the newest-sibling fallback.
type versionItem struct {
	entry          *filer_pb.Entry
	versionID      string
	bareKey        string // bucket-relative path; non-empty only for the null version
	isExplicitNull bool
}

// expandVersionsDir lists <root>/<key>/ and, when the children look like
// SeaweedFS version files, injects one reader.Event per version with
// BootstrapVersion populated. The bare logical key (the "null" version,
// living outside .versions/) is included as a sibling so:
//   - pre-versioning objects with a newer .versions/ history fire
//     NoncurrentDays as id="null"
//   - suspended-bucket writes (which clear the .versions/ latest pointer)
//     correctly classify null as the current version while every
//     .versions/ child becomes noncurrent
//
// versionsKey is the bucket-relative path of the .versions/ directory
// (e.g. "logs/foo.versions"). When the bare null version is included,
// its bucket-relative path is added to skipBare so the walker's regular
// emission for the same entry is suppressed.
//
// When no child has ExtVersionIdKey the directory is a coincidentally-
// named user folder; recurse via fallback (the bucket walk's own cb).
func (b *BucketBootstrapper) expandVersionsDir(ctx context.Context, bucket, root, versionsKey string, versionsEntry *filer_pb.Entry, fallback func(*filer_pb.Entry, string) error, skipBare map[string]bool) (int, error) {
	logical := strings.TrimSuffix(versionsKey, s3_constants.VersionsFolder)
	if logical == "" {
		return 0, nil
	}
	versionsDir := strings.TrimSuffix(b.BucketsPath, "/") + "/" + bucket + "/" + versionsKey
	// Collect file children only. Subdirectories under .versions/ would
	// corrupt sort/rank math; the disambiguation pass below also wants
	// to see only file-shaped children.
	var children []*filer_pb.Entry
	if err := filer_pb.SeaweedList(ctx, b.FilerClient, versionsDir, "", func(e *filer_pb.Entry, _ bool) error {
		if e != nil && e.Attributes != nil && !e.IsDirectory {
			children = append(children, e)
		}
		return nil
	}, "", false, 0); err != nil {
		return 0, fmt.Errorf("list %s: %w", versionsDir, err)
	}
	items := make([]versionItem, 0, len(children)+1)
	for _, e := range children {
		if id, ok := e.Extended[s3_constants.ExtVersionIdKey]; ok && len(id) > 0 {
			items = append(items, versionItem{entry: e, versionID: string(id)})
		}
	}
	if len(items) == 0 {
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
	// Look up the bare null version. SeaweedFS keeps it at the logical
	// path for pre-versioning objects and for suspended-bucket writes.
	// Both shapes count: regular file (PUT'd object) and explicit S3
	// directory-key marker (object name ends in /).
	if nullEntry, nullKey, explicit, ok := b.lookupNullVersion(ctx, bucket, logical); ok {
		items = append(items, versionItem{
			entry:          nullEntry,
			versionID:      "null",
			bareKey:        nullKey,
			isExplicitNull: explicit,
		})
	}
	// Sort newest-first: primary by mtime ns, fallback by version_id
	// (CompareVersionIds returns <0 when first arg is newer). PUTs only
	// set second-level Mtime, so collisions in the same second are
	// resolved by the canonical version-id ordering used elsewhere.
	sort.SliceStable(items, func(i, j int) bool {
		mi := items[i].entry.Attributes.Mtime*int64(1e9) + int64(items[i].entry.Attributes.MtimeNs)
		mj := items[j].entry.Attributes.Mtime*int64(1e9) + int64(items[j].entry.Attributes.MtimeNs)
		if mi != mj {
			return mi > mj
		}
		return s3lifecycle.CompareVersionIds(items[i].versionID, items[j].versionID) < 0
	})
	// Resolve latest position.
	//   1. Pointer names a real id -> that wins (in-order or backdated).
	//   2. Pointer absent + an EXPLICIT null exists (suspended write
	//      cleared the pointer and tagged the bare object as null) -> null
	//      is latest.
	//   3. Pointer absent + only an implicit null (pre-versioning bare
	//      object): treat as a race window with a new version write, fall
	//      back to the newest sibling so retention isn't unsafe.
	latestID := string(versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey])
	latestPos := 0
	if latestID != "" {
		for i, it := range items {
			if it.versionID == latestID {
				latestPos = i
				break
			}
		}
	} else {
		for i, it := range items {
			if it.versionID == "null" && it.isExplicitNull {
				latestPos = i
				break
			}
		}
	}
	count := 0
	for i, it := range items {
		var successor time.Time
		if i > 0 {
			prev := items[i-1].entry.Attributes
			successor = time.Unix(prev.Mtime, int64(prev.MtimeNs))
		}
		bv := &reader.BootstrapVersion{
			LogicalKey:       logical,
			VersionID:        it.versionID,
			IsLatest:         i == latestPos,
			IsDeleteMarker:   string(it.entry.Extended[s3_constants.ExtDeleteMarkerKey]) == "true",
			NumVersions:      len(items),
			SuccessorModTime: successor,
		}
		if !bv.IsLatest {
			rank := i
			if i > latestPos {
				rank = i - 1
			}
			bv.NoncurrentIndex = rank
		}
		// Event Key for bookkeeping: real version files keep the
		// .versions/<file> path; the null version uses its bare path
		// so the dispatcher's identity check resolves to the same
		// entry the walker would have emitted.
		evKey := versionsKey + "/" + it.entry.Name
		if it.versionID == "null" {
			evKey = it.bareKey
		}
		ev := &reader.Event{
			TsNs:             0,
			Bucket:           bucket,
			Key:              evKey,
			ShardID:          s3lifecycle.ShardID(bucket, logical),
			NewEntry:         it.entry,
			BootstrapVersion: bv,
		}
		if err := b.Injector.InjectEvent(ctx, ev); err != nil {
			return count, err
		}
		if it.versionID == "null" && skipBare != nil {
			skipBare[it.bareKey] = true
		}
		count++
	}
	return count, nil
}

// lookupNullVersion returns the bare-key entry that represents the null
// version of logical, if any. Both regular files and S3 directory-key
// markers (an empty directory entry with Mime set) qualify. The
// explicit return reports whether the entry's Extended map carries
// ExtVersionIdKey == "null" — the marker the suspended-versioning
// write path applies (s3api_object_handlers_put.go). bucketRelKey is
// the bucket-relative path the walker would otherwise emit, so the
// caller can suppress the duplicate.
func (b *BucketBootstrapper) lookupNullVersion(ctx context.Context, bucket, logical string) (entry *filer_pb.Entry, bucketRelKey string, explicit bool, ok bool) {
	bucketPath := strings.TrimSuffix(b.BucketsPath, "/") + "/" + bucket
	parent, name := util.NewFullPath(bucketPath, logical).DirAndName()
	resp, err := filer_pb.LookupEntry(ctx, b.FilerClient, &filer_pb.LookupDirectoryEntryRequest{
		Directory: parent,
		Name:      name,
	})
	if err != nil || resp == nil || resp.Entry == nil {
		return nil, "", false, false
	}
	e := resp.Entry
	if e.IsDirectory && !e.IsDirectoryKeyObject() {
		return nil, "", false, false
	}
	if id, hasID := e.Extended[s3_constants.ExtVersionIdKey]; hasID && string(id) == "null" {
		explicit = true
	}
	return e, strings.TrimPrefix(parent+"/"+name, bucketPath+"/"), explicit, true
}

// walkBucketDir lists every entry under dir and invokes cb. Two kinds
// of directories are emitted whole rather than recursed into:
//   - .uploads/<id> MPU init dirs (router fires ABORT_MPU off the dir entry)
//   - <key>.versions/ directories (caller expands them into per-version
//     events; recursing here would emit individual version files without
//     the sibling state needed for NoncurrentDays / NewerNoncurrent)
//
// .versions/ dirs are processed before everything else at each level so
// the cb's expandVersionsDir call can record the bare null-version key
// in the walk-shared skip set before the same level emits the bare entry.
func walkBucketDir(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, bucketRoot string, cb func(entry *filer_pb.Entry, key string) error) error {
	var children []*filer_pb.Entry
	if err := filer_pb.SeaweedList(ctx, client, dir, "", func(e *filer_pb.Entry, _ bool) error {
		children = append(children, e)
		return nil
	}, "", false, 0); err != nil {
		return fmt.Errorf("list %s: %w", dir, err)
	}
	// Pass 1: .versions/ directories. The cb expands them and may
	// claim a sibling bare key as the null version.
	for _, entry := range children {
		if entry == nil || entry.Attributes == nil {
			continue
		}
		if !entry.IsDirectory || !isVersionsDir(entry) {
			continue
		}
		full := dir + "/" + entry.Name
		key := strings.TrimPrefix(full, bucketRoot+"/")
		if err := cb(entry, key); err != nil {
			return err
		}
	}
	// Pass 2: everything else. Bare entries whose name was claimed by
	// a sibling .versions/ expansion are dropped by the cb's skip-set.
	for _, entry := range children {
		if entry == nil || entry.Attributes == nil {
			continue
		}
		if entry.IsDirectory && isVersionsDir(entry) {
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

