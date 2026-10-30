package command

import (
	"context"
	"errors"
	"path"
	"regexp"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
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
}

func isVersionsDirName(name string) bool {
	return strings.HasSuffix(name, s3_constants.VersionsFolder)
}

func insideVersionsDir(dir string) bool {
	return isVersionsDirName(path.Base(dir))
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
		return true, nil
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
		return false, nil
	}

	if newVersionsDir != "" {
		if err := f.applyLatest(util.FullPath(newVersionsDir), message.NewEntry, message.OldEntry == nil, message.Signatures); err != nil {
			return true, err
		}
	}
	// The versions directory is gone (deleted outright or renamed away):
	// whatever the flattened key held is no longer current.
	if oldVersionsDir != "" && oldVersionsDir != newVersionsDir {
		if err := f.deleteObject(objectKeyOf(util.FullPath(oldVersionsDir)), message.Signatures); err != nil {
			return true, err
		}
	}
	return true, nil
}

func objectKeyOf(versionsDir util.FullPath) util.FullPath {
	dir, name := versionsDir.DirAndName()
	return util.FullPath(dir).Child(strings.TrimSuffix(name, s3_constants.VersionsFolder))
}

// applyLatest writes the object a .versions pointer currently names to the
// flattened key: the latest version file, a delete marker (flat delete), the
// null object at the regular path, or — once every version is gone and the
// pointer is cleared — nothing.
func (f *s3VersionFlattener) applyLatest(versionsDir util.FullPath, dirEntry *filer_pb.Entry, isCreate bool, signatures []int32) error {
	objectKey := objectKeyOf(versionsDir)
	if !util.IsEqualOrUnder(string(objectKey), f.sourcePath) ||
		matchesExcludePath(string(objectKey), f.excludePaths) ||
		matchesExcludePath(string(versionsDir), f.excludePaths) ||
		isEntryExcluded(path.Dir(string(objectKey)), dirEntry, f.reExcludeFileName, f.excludeFileNames, f.excludePathPatterns) {
		return nil
	}

	extended := dirEntry.Extended
	switch {
	case string(extended[s3_constants.ExtLatestVersionIsDeleteMarker]) == "true":
		return f.deleteObject(objectKey, signatures)
	case string(extended[s3_constants.ExtNullVersionIsLatestKey]) == "true":
		return f.uploadLatest(objectKey, objectKey, signatures)
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
	return f.dataSink.CreateEntry(destKey(f.dataSink, f.targetPath, f.sourcePath, objectKey, mTime), entry, signatures)
}

func (f *s3VersionFlattener) deleteObject(objectKey util.FullPath, signatures []int32) error {
	if !f.doDeleteFiles || f.dataSink.IsIncremental() {
		return nil
	}
	return sink.DeleteEntry(f.dataSink, destKey(f.dataSink, f.targetPath, f.sourcePath, objectKey, 0), false, false, false, signatures)
}
