package dailyrun

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/bootstrap"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// listPageSize is the page size for paginated directory listings. The
// filer caps SeaweedList(..., limit=0) at DirListingLimit (1000 by
// default) per call, so a single-page list would silently truncate
// large directories. Atomic so tests can shrink it without racing.
var listPageSize atomic.Uint32

func init() { listPageSize.Store(1024) }

// FilerListFunc returns a bootstrap.ListFunc that streams entries
// under <bucketsPath>/<bucket>. Versioned siblings are expanded with
// IsLatest / NumVersions / NoncurrentIndex / SuccessorModTime so the
// walker's NoncurrentDays evaluation has the same per-version state
// the streaming bootstrap injects via reader.Event.BootstrapVersion.
// MPU init records at .uploads/<id> with ExtMultipartObjectKey set
// are emitted whole with IsMPUInit=true and DestKey carrying the
// user's intended path.
func FilerListFunc(client filer_pb.SeaweedFilerClient, bucketsPath string) bootstrap.ListFunc {
	return func(ctx context.Context, bucket, start string, cb func(*bootstrap.Entry) error) error {
		if client == nil {
			return fmt.Errorf("FilerListFunc: nil client")
		}
		root := strings.TrimSuffix(bucketsPath, "/") + "/" + bucket
		return walkBucketTree(ctx, client, root, root, start, cb)
	}
}

// walkBucketTree recurses through dir in two passes per level. Pass 1
// expands `.versions/` dirs (populating skipBare with the bare null-
// version keys that pass 2 must suppress). Pass 2 emits regular files
// and recurses into non-special subdirectories.
//
// The two-pass shape mirrors scheduler/bootstrap.go's walkBucketDir
// (see that file's comment for why `.versions/` has to be processed
// before its bare sibling).
func walkBucketTree(ctx context.Context, client filer_pb.SeaweedFilerClient, dir, bucketRoot, start string, cb func(*bootstrap.Entry) error) error {
	skipBare := map[string]bool{}

	// Pass 1: .versions/ dirs only.
	if err := listAll(ctx, client, dir, func(e *filer_pb.Entry) error {
		if e == nil || e.Attributes == nil {
			return nil
		}
		if !e.IsDirectory || !isVersionsDir(e) {
			return nil
		}
		full := dir + "/" + e.Name
		key := strings.TrimPrefix(full, bucketRoot+"/")
		return expandVersionsDir(ctx, client, bucketRoot, key, e, start, skipBare, cb)
	}); err != nil {
		return err
	}

	// Pass 2: everything else.
	return listAll(ctx, client, dir, func(e *filer_pb.Entry) error {
		if e == nil || e.Attributes == nil {
			return nil
		}
		if e.IsDirectory && isVersionsDir(e) {
			return nil
		}
		full := dir + "/" + e.Name
		key := strings.TrimPrefix(full, bucketRoot+"/")
		if e.IsDirectory {
			if isMPUInitDir(key, e) {
				if start != "" && key <= start {
					return nil
				}
				destKey := string(e.Extended[s3_constants.ExtMultipartObjectKey])
				return cb(&bootstrap.Entry{
					Path:      key,
					DestKey:   destKey,
					IsMPUInit: true,
					ModTime:   time.Unix(e.Attributes.Mtime, int64(e.Attributes.MtimeNs)),
					Size:      int64(e.Attributes.FileSize),
				})
			}
			return walkBucketTree(ctx, client, full, bucketRoot, start, cb)
		}
		if skipBare[key] {
			return nil
		}
		if start != "" && key <= start {
			return nil
		}
		entry := &bootstrap.Entry{
			Path:     key,
			ModTime:  time.Unix(e.Attributes.Mtime, int64(e.Attributes.MtimeNs)),
			Size:     int64(e.Attributes.FileSize),
			IsLatest: true, // Non-versioned default.
		}
		return cb(entry)
	})
}

// versionItem captures one sibling of a `.versions/` expansion. bareKey
// is the bucket-relative path of the bare null-version entry when the
// item represents it; for real version files it stays empty.
type versionItem struct {
	entry          *filer_pb.Entry
	versionID      string
	bareKey        string
	isExplicitNull bool
}

// expandVersionsDir handles the `.versions/<key>` directory. Lists
// version files, optionally appends the bare null-version sibling,
// sorts newest-first, resolves the latest, and emits one Entry per
// version with the sibling state walkEntry needs to evaluate
// NoncurrentDays / NewerNoncurrent / ExpirationDays correctly.
//
// Ported from scheduler/bootstrap.go's same-named helper; both must
// agree on sort, latest resolution, and successor derivation so the
// streaming and walker paths reach the same verdict for the same
// objects. Phase 5 deletes the scheduler copy.
//
// Resume note: every emitted sibling shares Path = logical key, so a
// resume after a mid-expansion failure rewalks the whole sibling
// group. Acceptable today because Phase 4b doesn't persist a
// Checkpoint between runs (start is always "" via runShard).
func expandVersionsDir(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketRoot, versionsKey string, versionsEntry *filer_pb.Entry, start string, skipBare map[string]bool, cb func(*bootstrap.Entry) error) error {
	logical := strings.TrimSuffix(versionsKey, s3_constants.VersionsFolder)
	if logical == "" {
		return nil
	}
	versionsDir := bucketRoot + "/" + versionsKey
	var children []*filer_pb.Entry
	if err := listAll(ctx, client, versionsDir, func(e *filer_pb.Entry) error {
		if e != nil && e.Attributes != nil && !e.IsDirectory {
			children = append(children, e)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("list %s: %w", versionsDir, err)
	}
	items := make([]versionItem, 0, len(children)+1)
	for _, e := range children {
		if id, ok := e.Extended[s3_constants.ExtVersionIdKey]; ok && len(id) > 0 {
			items = append(items, versionItem{entry: e, versionID: string(id)})
		}
	}
	if len(items) == 0 {
		// Coincidentally-named user folder, or an empty `.versions`
		// container. Treat it as a regular subdirectory so user-named
		// files inside still surface.
		return walkBucketTree(ctx, client, versionsDir, bucketRoot, start, cb)
	}
	if nullEntry, nullKey, explicit, ok := lookupNullVersion(ctx, client, bucketRoot, logical); ok {
		items = append(items, versionItem{
			entry:          nullEntry,
			versionID:      "null",
			bareKey:        nullKey,
			isExplicitNull: explicit,
		})
	}

	// Sort newest-first by mtime, ties broken by version_id (newer wins).
	sort.SliceStable(items, func(i, j int) bool {
		mi := items[i].entry.Attributes.Mtime*int64(1e9) + int64(items[i].entry.Attributes.MtimeNs)
		mj := items[j].entry.Attributes.Mtime*int64(1e9) + int64(items[j].entry.Attributes.MtimeNs)
		if mi != mj {
			return mi > mj
		}
		return s3lifecycle.CompareVersionIds(items[i].versionID, items[j].versionID) < 0
	})

	// Resolve latest:
	//   1. Pointer names a real id -> that wins.
	//   2. Pointer absent (or stale: set but no sibling carries it)
	//      + items[0] is an EXPLICIT null -> null is latest.
	//   3. Otherwise -> newest sibling (latestPos = 0 by default).
	//
	// A stale pointer falls through to the no-pointer fallback rather
	// than silently leaving latestPos at 0 with no documented intent;
	// the value happens to be the same today (newest sibling wins
	// either way) but the explicit branching protects against future
	// fallback refinements diverging by accident.
	latestID := string(versionsEntry.Extended[s3_constants.ExtLatestVersionIdKey])
	latestPos := 0
	pointerResolved := false
	if latestID != "" {
		for i, it := range items {
			if it.versionID == latestID {
				latestPos = i
				pointerResolved = true
				break
			}
		}
	}
	if !pointerResolved && len(items) > 0 && items[0].versionID == "null" && items[0].isExplicitNull {
		latestPos = 0
	}

	if start != "" && logical <= start {
		// All siblings share Path=logical, so the whole group is
		// either above or below the resume marker.
		return nil
	}
	for i, it := range items {
		successor := s3lifecycle.SuccessorFromEntryStamp(it.entry)
		if successor.IsZero() && i > 0 {
			prev := items[i-1].entry.Attributes
			successor = time.Unix(prev.Mtime, int64(prev.MtimeNs))
		}
		isLatest := i == latestPos
		entry := &bootstrap.Entry{
			Path:             logical,
			VersionID:        it.versionID,
			ModTime:          time.Unix(it.entry.Attributes.Mtime, int64(it.entry.Attributes.MtimeNs)),
			Size:             int64(it.entry.Attributes.FileSize),
			IsLatest:         isLatest,
			IsDeleteMarker:   string(it.entry.Extended[s3_constants.ExtDeleteMarkerKey]) == "true",
			NumVersions:      len(items),
			SuccessorModTime: successor,
		}
		if !isLatest {
			rank := i
			if i > latestPos {
				rank = i - 1
			}
			entry.NoncurrentIndex = &rank
		}
		if err := cb(entry); err != nil {
			return err
		}
		if it.versionID == "null" && skipBare != nil {
			skipBare[it.bareKey] = true
		}
	}
	return nil
}

// lookupNullVersion returns the bare-key entry that represents the null
// version of logical, if any. Both regular files and S3 directory-key
// markers qualify. explicit is true when the entry carries
// ExtVersionIdKey == "null" — the marker the suspended-versioning
// write path applies; only an explicit-null bare can outrank a missing
// `.versions/` pointer per the latest-resolution rules above.
func lookupNullVersion(ctx context.Context, client filer_pb.SeaweedFilerClient, bucketRoot, logical string) (*filer_pb.Entry, string, bool, bool) {
	parent, name := util.NewFullPath(bucketRoot, logical).DirAndName()
	resp, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
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
	explicit := false
	if id, hasID := e.Extended[s3_constants.ExtVersionIdKey]; hasID && string(id) == "null" {
		explicit = true
	}
	return e, strings.TrimPrefix(parent+"/"+name, bucketRoot+"/"), explicit, true
}

// listAll issues paginated SeaweedList calls until exhausted. Ported
// from scheduler/bootstrap.go's same-named helper; Phase 5 deletes
// the scheduler copy when the streaming path is removed.
func listAll(ctx context.Context, client filer_pb.SeaweedFilerClient, dir string, fn func(*filer_pb.Entry) error) error {
	pageSize := listPageSize.Load()
	startFrom := ""
	for {
		var pageCount uint32
		var lastName string
		if err := filer_pb.SeaweedList(ctx, client, dir, "", func(e *filer_pb.Entry, _ bool) error {
			pageCount++
			if e != nil {
				lastName = e.Name
			}
			return fn(e)
		}, startFrom, false, pageSize); err != nil {
			return err
		}
		if pageCount < pageSize {
			return nil
		}
		startFrom = lastName
	}
}

func isVersionsDir(entry *filer_pb.Entry) bool {
	return entry.IsDirectory && strings.HasSuffix(entry.Name, s3_constants.VersionsFolder)
}

// isMPUInitDir mirrors router.mpuInitInfo: a directory at
// .uploads/<id> carrying the destination key in Extended is the MPU
// init record. Verified shape + presence of ExtMultipartObjectKey;
// directories at .uploads/<id> without the key are mid-write before
// metadata landed and stay out of the dispatch path.
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
