package command

import (
	"context"
	"errors"
	"path"
	"regexp"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/wildcard"
)

// s3VersionFlattener mirrors the current S3 view of versioned buckets into the
// sink: under each "<key>.versions" directory only the latest version is
// written to "<key>", so the destination always holds a plain restorable copy.
// Version files never reach the sink themselves — the latest-version pointer
// on the .versions directory decides what the flattened key holds.
type s3VersionFlattener struct {
	filerSource         filer_pb.FilerClient
	dataSink            sink.ReplicationSink
	sourcePath          string
	targetPath          string
	doDeleteFiles       bool
	excludePaths        []string
	reExcludeFileName   *regexp.Regexp
	excludeFileNames    []*wildcard.WildcardMatcher
	excludePathPatterns []*wildcard.WildcardMatcher
	flatLocks           [256]sync.Mutex
}

func isVersionsDirName(name string) bool {
	return strings.HasSuffix(name, s3_constants.VersionsFolder)
}

func insideVersionsDir(dir string) bool {
	return isVersionsDirName(path.Base(dir))
}

// lockFor serializes the handling of one event by the key the flattened mirror
// writes to: versions-related events lock their object key, everything else its
// own path, so a pointer update and a regular-path write to the same object
// cannot overlap. Both sides of the event are locked, since a rename or delete
// can retire one flattened key while writing another. Non-barrier dir updates
// run concurrently in the metadata processor, which would otherwise let an
// older flattened upload finish last.
func (f *s3VersionFlattener) lockFor(resp *filer_pb.SubscribeMetadataResponse) func() {
	oldKey, newKey := eventFlatKeys(resp)
	if oldKey == "" {
		oldKey = newKey
	}
	if newKey == "" {
		newKey = oldKey
	}
	if newKey == "" {
		oldKey, newKey = resp.Directory, resp.Directory
	}
	a := util.HashStringToLong(oldKey) & int64(len(f.flatLocks)-1)
	b := util.HashStringToLong(newKey) & int64(len(f.flatLocks)-1)
	if a == b {
		f.flatLocks[a].Lock()
		return f.flatLocks[a].Unlock
	}
	if b < a {
		a, b = b, a
	}
	f.flatLocks[a].Lock()
	f.flatLocks[b].Lock()
	return func() {
		f.flatLocks[b].Unlock()
		f.flatLocks[a].Unlock()
	}
}

func eventFlatKeys(resp *filer_pb.SubscribeMetadataResponse) (string, string) {
	message := resp.EventNotification
	return flatKeyOf(resp.Directory, message.OldEntry),
		flatKeyOf(filer_pb.MetadataEventTargetDirectory(resp), message.NewEntry)
}

func flatKeyOf(dir string, entry *filer_pb.Entry) string {
	if entry == nil {
		return ""
	}
	if insideVersionsDir(dir) && strings.HasPrefix(entry.Name, "v_") {
		return string(objectKeyOf(util.FullPath(dir)))
	}
	if entry.IsDirectory && isVersionsDirName(entry.Name) {
		return string(objectKeyOf(util.FullPath(dir).Child(entry.Name)))
	}
	return string(util.FullPath(dir).Child(entry.Name))
}

// process handles the versions-related part of one metadata event and reports
// whether the event was consumed. Version files under a .versions directory
// are dropped outright; create/update/delete of the .versions directory
// itself is mapped onto the flattened object key through its latest-version
// pointer. Anything else is left for the generic event path.
func (f *s3VersionFlattener) process(resp *filer_pb.SubscribeMetadataResponse) (bool, error) {
	message := resp.EventNotification
	if message == nil {
		return false, nil
	}
	targetDir := filer_pb.MetadataEventTargetDirectory(resp)

	name := ""
	if message.NewEntry != nil {
		name = message.NewEntry.Name
	} else if message.OldEntry != nil {
		name = message.OldEntry.Name
	}
	if strings.HasPrefix(name, "v_") && (insideVersionsDir(resp.Directory) || insideVersionsDir(targetDir)) {
		// A version file changed; only a change to the version the pointer
		// still names can move the flattened key (e.g. a tag update), so
		// re-resolve the pointer and refresh just for that child.
		versionsDir := resp.Directory
		if !insideVersionsDir(versionsDir) {
			versionsDir = targetDir
		}
		dirEntry, _, _, err := filer_pb.GetEntry(context.Background(), f.filerSource, util.FullPath(versionsDir))
		if err != nil {
			if errors.Is(err, filer_pb.ErrNotFound) {
				return true, nil
			}
			return true, err
		}
		if name != string(dirEntry.Extended[s3_constants.ExtLatestVersionFileNameKey]) {
			return true, nil
		}
		return true, f.applyLatest(util.FullPath(versionsDir), dirEntry, false, message.Signatures)
	}

	oldVersionsDir := ""
	newVersionsDir := ""
	if message.OldEntry != nil && message.OldEntry.IsDirectory && isVersionsDirName(message.OldEntry.Name) {
		oldVersionsDir = string(util.FullPath(resp.Directory).Child(message.OldEntry.Name))
	}
	if message.NewEntry != nil && message.NewEntry.IsDirectory && isVersionsDirName(message.NewEntry.Name) {
		newVersionsDir = string(util.FullPath(targetDir).Child(message.NewEntry.Name))
	}
	if oldVersionsDir == "" && newVersionsDir == "" {
		return f.processRegularPath(resp, targetDir, message)
	}

	if newVersionsDir != "" {
		if err := f.applyLatest(util.FullPath(newVersionsDir), message.NewEntry, message.OldEntry == nil, message.Signatures); err != nil {
			return true, err
		}
	}
	// The versions directory is gone (deleted outright or renamed away):
	// whatever the flattened key held is no longer current.
	if oldVersionsDir != "" && oldVersionsDir != newVersionsDir {
		objectKey := objectKeyOf(util.FullPath(oldVersionsDir))
		if f.isExcludedObject(objectKey, util.FullPath(oldVersionsDir), message.OldEntry) {
			return true, nil
		}
		if err := f.deleteObject(objectKey, message.Signatures); err != nil {
			return true, err
		}
	}
	return true, nil
}

func objectKeyOf(versionsDir util.FullPath) util.FullPath {
	dir, name := versionsDir.DirAndName()
	return util.FullPath(dir).Child(strings.TrimSuffix(name, s3_constants.VersionsFolder))
}

// processRegularPath guards the generic event path for keys that carry a
// .versions directory: a regular-path entry (the null object, or its delete)
// can coexist with a pointer that names a live version, and only the pointer
// decides the flattened key — e.g. deleting the null object must not remove a
// flattened key whose current version is live. A rename touches two keys, so
// each side is resolved on its own; when exactly one side is versioned the
// other is handed to the generic path as a lone delete or create. Keys with
// no .versions sibling are left to the generic path entirely.
func (f *s3VersionFlattener) processRegularPath(resp *filer_pb.SubscribeMetadataResponse, targetDir string, message *filer_pb.EventNotification) (bool, error) {
	oldHandled, err := f.refreshIfVersioned(util.FullPath(resp.Directory), message.OldEntry, message.Signatures)
	if err != nil {
		return true, err
	}
	newHandled := oldHandled
	if !filer_pb.IsUpdate(resp) {
		newHandled, err = f.refreshIfVersioned(util.FullPath(targetDir), message.NewEntry, message.Signatures)
		if err != nil {
			return true, err
		}
	}
	if !filer_pb.IsRename(resp) || oldHandled == newHandled {
		return oldHandled || newHandled, nil
	}
	if oldHandled {
		// The flattened old key keeps whatever its pointer still names; the
		// generic path only needs to create the moved entry.
		message.OldEntry = nil
	} else {
		// The flattened old key held the moved file; the generic path only
		// needs to delete it.
		message.NewEntry = nil
	}
	return false, nil
}

// refreshIfVersioned re-resolves the flattened key of dir/entry.Name when a
// .versions directory sits next to it, reporting whether that sibling exists.
// The pointer decides what the flattened key holds, so a regular-path event
// only refreshes through it and never writes the flattened key directly.
func (f *s3VersionFlattener) refreshIfVersioned(dir util.FullPath, entry *filer_pb.Entry, signatures []int32) (bool, error) {
	if entry == nil || entry.IsDirectory {
		return false, nil
	}
	versionsDir := dir.Child(entry.Name + s3_constants.VersionsFolder)
	dirEntry, _, _, err := filer_pb.GetEntry(context.Background(), f.filerSource, versionsDir)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return false, nil
		}
		return true, err
	}
	return true, f.applyLatest(versionsDir, dirEntry, false, signatures)
}

// applyLatest writes the object a .versions pointer currently names to the
// flattened key: the latest version file, a delete marker (flat delete), the
// null object at the regular path, or — once every version is gone and the
// pointer is cleared — nothing.
func (f *s3VersionFlattener) applyLatest(versionsDir util.FullPath, dirEntry *filer_pb.Entry, isCreate bool, signatures []int32) error {
	objectKey := objectKeyOf(versionsDir)
	if f.isExcludedObject(objectKey, versionsDir, dirEntry) {
		return nil
	}

	extended := dirEntry.Extended
	switch {
	case string(extended[s3_constants.ExtLatestVersionIsDeleteMarker]) == "true":
		return f.deleteObject(objectKey, signatures)
	case string(extended[s3_constants.ExtNullVersionIsLatestKey]) == "true":
		return f.uploadNullLatest(objectKey, versionsDir, signatures)
	default:
		if latestName := string(extended[s3_constants.ExtLatestVersionFileNameKey]); latestName != "" {
			return f.uploadLatest(objectKey, versionsDir.Child(latestName), signatures)
		}
		// A create event lands before the pointer is written; an update with
		// no pointer means the last version was removed.
		if isCreate {
			return nil
		}
		return f.deleteObject(objectKey, signatures)
	}
}

// uploadNullLatest flattens the regular-path null object while the pointer
// names it latest. A ?versionId=null delete removes that object without
// touching the pointer, so on a miss the newest remaining version wins (or a
// flat delete when none survives) — the same rescan the S3 read path falls
// back to.
func (f *s3VersionFlattener) uploadNullLatest(objectKey, versionsDir util.FullPath, signatures []int32) error {
	if _, _, _, err := filer_pb.GetEntry(context.Background(), f.filerSource, objectKey); err == nil {
		return f.uploadLatest(objectKey, objectKey, signatures)
	} else if !errors.Is(err, filer_pb.ErrNotFound) {
		return err
	}
	latestName, err := f.latestVersionFileName(versionsDir)
	if err != nil {
		return err
	}
	if latestName == "" {
		return f.deleteObject(objectKey, signatures)
	}
	return f.uploadLatest(objectKey, versionsDir.Child(latestName), signatures)
}

// latestVersionFileName returns the name of the newest version file under
// versionsDir by version-id order — delete markers included, the upload
// resolves them to a flat delete — or "" when no version entry remains. It
// paginates: the filer caps one request at DirListingLimit, and old-format
// version names sort oldest-first, so a single page can miss the true latest.
func (f *s3VersionFlattener) latestVersionFileName(versionsDir util.FullPath) (string, error) {
	var latestName, latestId string
	err := f.filerSource.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		startFrom := ""
		for {
			var lastName string
			var pageCount uint32
			if err := filer_pb.SeaweedList(context.Background(), client, string(versionsDir), "", func(entry *filer_pb.Entry, _ bool) error {
				pageCount++
				if entry == nil {
					return nil
				}
				lastName = entry.Name
				versionId := s3api.VersionIdFromEntry(entry)
				if versionId != "" && (latestId == "" || s3api.CompareVersionIds(versionId, latestId) < 0) {
					latestId, latestName = versionId, entry.Name
				}
				return nil
			}, startFrom, false, filer.PaginationSize); err != nil {
				return err
			}
			if pageCount < filer.PaginationSize || lastName == "" {
				return nil
			}
			startFrom = lastName
		}
	})
	return latestName, err
}

func (f *s3VersionFlattener) uploadLatest(objectKey, sourceKey util.FullPath, signatures []int32) error {
	entry, _, _, err := filer_pb.GetEntry(context.Background(), f.filerSource, sourceKey)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			// The pointed-at version (or null object) is not readable yet or is
			// already gone again; its own event or a later pointer update
			// converges the flattened key.
			glog.V(1).Infof("flatten %s: source %s not found: %v", objectKey, sourceKey, err)
			return nil
		}
		return err
	}
	if string(entry.Extended[s3_constants.ExtDeleteMarkerKey]) == "true" {
		return f.deleteObject(objectKey, signatures)
	}
	var mTime int64
	if entry.Attributes != nil {
		mTime = entry.Attributes.Mtime
	}
	return sink.CreateEntryLatest(f.dataSink, destKey(f.dataSink, f.targetPath, f.sourcePath, objectKey, mTime), entry, signatures)
}

// isExcludedObject applies the source scope and exclusion rules to the
// flattened object key — including its own name, so a file-name pattern that
// excludes "k" also excludes the flattened "k" even though the event names
// "k.versions".
func (f *s3VersionFlattener) isExcludedObject(objectKey, versionsDir util.FullPath, dirEntry *filer_pb.Entry) bool {
	dir, name := objectKey.DirAndName()
	return !util.IsEqualOrUnder(string(objectKey), f.sourcePath) ||
		matchesExcludePath(string(objectKey), f.excludePaths) ||
		matchesExcludePath(string(versionsDir), f.excludePaths) ||
		isEntryExcluded(dir, dirEntry, f.reExcludeFileName, f.excludeFileNames, f.excludePathPatterns) ||
		isEntryExcluded(dir, &filer_pb.Entry{Name: name}, f.reExcludeFileName, f.excludeFileNames, f.excludePathPatterns)
}

func (f *s3VersionFlattener) deleteObject(objectKey util.FullPath, signatures []int32) error {
	if !f.doDeleteFiles || f.dataSink.IsIncremental() {
		return nil
	}
	return sink.DeleteEntry(f.dataSink, destKey(f.dataSink, f.targetPath, f.sourcePath, objectKey, 0), false, true, false, signatures)
}
