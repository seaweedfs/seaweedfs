package filer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	MsgFailDelNonEmptyFolder = "fail to delete non-empty folder"
	MsgFailDelLockedEntry    = "fail to delete locked entry"
)

// ErrNonEmptyFolder is a non-recursive delete refused because the folder still
// has children. The marker leads the message the filer builds for it and is
// never wrapped on the way out, so the path that follows it, which the client
// chose, cannot forge one.
var ErrNonEmptyFolder = errors.New(MsgFailDelNonEmptyFolder)

// ErrEntryLocked is a delete refused because the entry carries an active legal
// hold or an unexpired retention. It follows ErrNonEmptyFolder's leading-marker
// rule for the same reason.
var ErrEntryLocked = errors.New(MsgFailDelLockedEntry)

// DeleteEntryError turns the text of DeleteEntryResponse.Error back into an
// error carrying the condition the filer reported. Call it on the response
// field, before formatting a path around it.
func DeleteEntryError(msg string) error {
	if strings.HasPrefix(msg, MsgFailDelNonEmptyFolder) {
		return &deleteEntryError{msg: msg, cause: ErrNonEmptyFolder}
	}
	if strings.HasPrefix(msg, MsgFailDelLockedEntry) {
		return &deleteEntryError{msg: msg, cause: ErrEntryLocked}
	}
	return errors.New(msg)
}

// IsNonEmptyFolderError is for callers holding a delete failure that has not
// been wrapped yet: the sentinel when it survived, the leading marker when the
// error only crossed the wire as text.
func IsNonEmptyFolderError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrNonEmptyFolder) || strings.HasPrefix(err.Error(), MsgFailDelNonEmptyFolder)
}

// IsEntryLockedError is the ErrEntryLocked counterpart of IsNonEmptyFolderError.
func IsEntryLockedError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ErrEntryLocked) || strings.HasPrefix(err.Error(), MsgFailDelLockedEntry)
}

type deleteEntryError struct {
	msg   string
	cause error
}

func (e *deleteEntryError) Error() string { return e.msg }
func (e *deleteEntryError) Unwrap() error { return e.cause }

// ExtKeepRemoteObjectKey marks the entry in the event of a metadata-only
// delete, so the remote write-back daemons leave the remote object alone too.
// It is set on the event only; the store drops it from every write, so a
// client cannot plant it on an entry.
const ExtKeepRemoteObjectKey = "Seaweed-X-Keep-Remote-Object"

// IsMetadataOnlyDelete reports a delete event's entry marked by
// WithKeepRemoteObject.
func IsMetadataOnlyDelete(oldEntry *filer_pb.Entry) bool {
	_, marked := oldEntry.GetExtended()[ExtKeepRemoteObjectKey]
	return marked
}

type keepRemoteObjectKey struct{}

// WithKeepRemoteObject marks a delete as metadata-only: neither the filer nor
// filer.remote.sync deletes the entry's object on the mounted remote storage.
// Unlike isFromOtherCluster, the metadata event stays an ordinary local
// delete, so replication still carries it.
func WithKeepRemoteObject(ctx context.Context) context.Context {
	return context.WithValue(ctx, keepRemoteObjectKey{}, true)
}

func keepsRemoteObject(ctx context.Context) bool {
	return ctx.Value(keepRemoteObjectKey{}) != nil
}

type bypassGovernanceRetentionKey struct{}

// WithBypassGovernanceRetention records that the caller is authorized to drop
// governance-mode retention on this delete -- the S3 gateway sets it after
// s3:BypassGovernanceRetention. Legal hold and compliance retention refuse
// the delete regardless.
func WithBypassGovernanceRetention(ctx context.Context) context.Context {
	return context.WithValue(ctx, bypassGovernanceRetentionKey{}, true)
}

// BypassesGovernanceRetention reports whether ctx carries the governance-bypass
// assertion WithBypassGovernanceRetention plants.
func BypassesGovernanceRetention(ctx context.Context) bool {
	return ctx.Value(bypassGovernanceRetentionKey{}) != nil
}

// entryLockedForDelete mirrors the S3 gateway's EntryHasActiveLock on the
// entry's extended attributes, except a governance bypass skips only
// governance-mode retention. An unparseable retention date fails closed.
func entryLockedForDelete(entry *Entry, bypassGovernance bool, now time.Time) bool {
	ext := entry.Extended
	if ext == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(string(ext[s3_constants.ExtLegalHoldKey])), s3_constants.LegalHoldOn) {
		return true
	}
	mode := strings.ToUpper(strings.TrimSpace(string(ext[s3_constants.ExtObjectLockModeKey])))
	if mode != s3_constants.RetentionModeCompliance && mode != s3_constants.RetentionModeGovernance {
		return false
	}
	if bypassGovernance && mode == s3_constants.RetentionModeGovernance {
		return false
	}
	dateBytes, found := ext[s3_constants.ExtRetentionUntilDateKey]
	if !found {
		return false
	}
	timestamp, err := strconv.ParseInt(strings.TrimSpace(string(dateBytes)), 10, 64)
	if err != nil {
		glog.Warningf("unparseable retention date %q on %s, treating as locked: %v", dateBytes, entry.FullPath, err)
		return true
	}
	return time.Unix(timestamp, 0).After(now)
}

func entryDeleteLockError(ctx context.Context, entry *Entry) error {
	if entryLockedForDelete(entry, BypassesGovernanceRetention(ctx), time.Now()) {
		return fmt.Errorf("%w: %s", ErrEntryLocked, entry.FullPath)
	}
	return nil
}

// lockedDescendantError walks the subtree once, refusing the delete on the
// first entry under an active legal hold or retention. It runs before the
// recursive delete emits any side effect, so a refusal cannot arrive after
// earlier descendants -- or the ancestor's remote tombstone -- are already
// gone, and it covers the store-drop path whose DeleteFolderChildren never
// lists the children it removes. The per-page check in the delete walk still
// catches a lock planted between this pass and the delete.
func (f *Filer) lockedDescendantError(ctx context.Context, entry *Entry) error {
	lastFileName := ""
	includeLastFile := false
	for {
		entries, _, err := f.ListDirectoryEntries(ctx, entry.FullPath, lastFileName, includeLastFile, PaginationSize, "", "", "")
		if err != nil {
			return fmt.Errorf("list folder %s: %v", entry.FullPath, err)
		}
		for _, sub := range entries {
			lastFileName = sub.Name()
			if lockErr := entryDeleteLockError(ctx, sub); lockErr != nil {
				return lockErr
			}
			if sub.IsDirectory() {
				if lockErr := f.lockedDescendantError(ctx, sub); lockErr != nil {
					return lockErr
				}
			}
		}
		if len(entries) < PaginationSize {
			return nil
		}
	}
}

// objectLockEnabledOnEntry mirrors the S3 gateway's IsObjectLockEnabled.
func objectLockEnabledOnEntry(entry *Entry) bool {
	enabled, found := entry.Extended[s3_constants.ExtObjectLockEnabledKey]
	if !found {
		return false
	}
	v := string(enabled)
	return v == s3_constants.ObjectLockEnabled || v == "true"
}

// emptyFolderDeleteContext makes deleting a folder that is empty in the filer
// metadata-only when it is under a remote mount. A mount is populated lazily,
// so the folder can still hold remote objects never read, and removing its
// remote directory deletes every object under that prefix.
func (f *Filer) emptyFolderDeleteContext(ctx context.Context, dirPath util.FullPath) context.Context {
	if f.IsUnderRemoteMount(dirPath) {
		return WithKeepRemoteObject(ctx)
	}
	return ctx
}

// IsUnderRemoteMount reports whether p is a remote mount directory or lies under one.
func (f *Filer) IsUnderRemoteMount(p util.FullPath) bool {
	if f.RemoteStorage == nil {
		return false
	}
	// Mount rules are keyed with a trailing slash, so this also matches the mount directory itself.
	_, remoteLoc := f.RemoteStorage.FindMountDirectory(util.FullPath(string(p) + "/"))
	return remoteLoc != nil
}

func deleteEventEntry(ctx context.Context, entry *Entry) *Entry {
	if !keepsRemoteObject(ctx) {
		return entry
	}
	marked := entry.ShallowClone()
	marked.Extended = maps.Clone(entry.Extended)
	if marked.Extended == nil {
		marked.Extended = map[string][]byte{}
	}
	marked.Extended[ExtKeepRemoteObjectKey] = []byte("true")
	return marked
}

type OnChunksFunc func([]*filer_pb.FileChunk) error
type OnHardLinkIdsFunc func([]HardLinkId) error

func (f *Filer) DeleteEntryMetaAndData(ctx context.Context, p util.FullPath, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isFromOtherCluster bool, signatures []int32, ifNotModifiedAfter int64) (err error) {
	if p == "/" {
		return nil
	}

	ctx = f.syncWriteContext(ctx, p)

	entry, findErr := f.FindEntry(ctx, p)
	if findErr != nil {
		return findErr
	}
	if ifNotModifiedAfter > 0 && entry.Attr.Mtime.Unix() > ifNotModifiedAfter {
		return nil
	}
	// A cluster replaying a delete the origin already authorized must not
	// second-guess it: the entry's lock state here may legitimately still be
	// in force (e.g. an expired-elsewhere retention, or a governance bypass
	// the peer observed before shipping the event).
	if !isFromOtherCluster {
		if lockErr := entryDeleteLockError(ctx, entry); lockErr != nil {
			return lockErr
		}
	}
	if !isRecursive && entry.IsDirectory() {
		ctx = f.emptyFolderDeleteContext(ctx, p)
	}
	isDeleteCollection := f.IsBucket(entry)
	collectionName := ""
	if isDeleteCollection {
		collectionName = f.bucketCollection(ctx, entry.Name())
	}
	// A preserved collection outlives the bucket, so its chunks are collected
	// per entry rather than dropped wholesale with it.
	dropsCollection := isDeleteCollection && collectionName != ""
	if entry.IsDirectory() {
		if isRecursive && !isFromOtherCluster {
			if lockErr := f.lockedDescendantError(ctx, entry); lockErr != nil {
				return lockErr
			}
		}
		// delete the folder children, not including the folder itself
		err = f.doBatchDeleteFolderMetaAndData(ctx, entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks && !dropsCollection, isDeleteCollection && (dropsCollection || !shouldDeleteChunks), isFromOtherCluster, signatures, func(hardLinkIds []HardLinkId) error {
			// A case not handled:
			// what if the chunk is in a different collection?
			if shouldDeleteChunks {
				f.maybeDeleteHardLinks(ctx, hardLinkIds)
			}
			return nil
		})
		if err != nil {
			glog.V(2).InfofCtx(ctx, "delete directory %s: %v", p, err)
			if errors.Is(err, ErrNonEmptyFolder) || errors.Is(err, ErrEntryLocked) {
				return err
			}
			return fmt.Errorf("delete directory %s: %v", p, err)
		}
	}

	// delete the file or folder
	err = f.doDeleteEntryMetaAndData(ctx, entry, shouldDeleteChunks, isFromOtherCluster, signatures)
	if err != nil {
		if errors.Is(err, ErrEntryLocked) {
			return err
		}
		return fmt.Errorf("delete file %s: %v", p, err)
	}

	if shouldDeleteChunks && !dropsCollection {
		if len(entry.HardLinkId) != 0 && entry.HardLinkCounter > 1 {
			// if the file is a hard link and there are other hard links, do not delete the chunks
		} else {
			f.DeleteChunks(ctx, p, entry.GetChunks())
		}
	}

	if isDeleteCollection {
		if collectionName != "" {
			// the entry is already gone: a caller that hung up must not leave the
			// collection behind, so this cleanup outlives the request -- bounded all
			// the same, or a master that is down parks this handler indefinitely and
			// every client retry behind it parks another
			collectionCtx, cancelCollection := context.WithTimeout(context.WithoutCancel(ctx), collectionDeleteTimeout)
			f.DoDeleteCollection(collectionCtx, collectionName)
			cancelCollection()
		}
		// drop bucket-labeled series held by this process; the S3 gateway
		// only cleans its own registry
		stats.DeleteBucketMetrics(entry.Name())
	}

	return nil
}

func (f *Filer) doBatchDeleteFolderMetaAndData(ctx context.Context, entry *Entry, isRecursive, ignoreRecursiveError, shouldDeleteChunks, isDeletingBucket, isFromOtherCluster bool, signatures []int32, onHardLinkIdsFn OnHardLinkIdsFunc) (err error) {

	var dirTombstoneTs int64
	if isRecursive && !keepsRemoteObject(ctx) {
		// Tombstone the directory before its children: when the store drops
		// the subtree without listing it, or a child error aborts the walk,
		// the ancestor tombstone still covers every descendant.
		dirTombstoneTs = time.Now().UnixNano()
		f.noteRemoteDeletion(entry.FullPath, true, dirTombstoneTs)
	}

	//collect all the chunks of this layer and delete them together at the end
	var chunksToDelete []*filer_pb.FileChunk
	lastFileName := ""
	includeLastFile := false
	// A lock-enabled bucket must be walked even when the store could drop it
	// whole: entries under active retention or legal hold refuse the delete.
	listedChildren := !isDeletingBucket || !f.Store.CanDropWholeBucket() || objectLockEnabledOnEntry(entry)
	if listedChildren {
		for {
			entries, _, err := f.ListDirectoryEntries(ctx, entry.FullPath, lastFileName, includeLastFile, PaginationSize, "", "", "")
			if err != nil {
				// nothing was deleted; a leftover tombstone would hide the
				// still-existing remote children
				f.unnoteRemoteDeletion(entry.FullPath, dirTombstoneTs)
				glog.ErrorfCtx(ctx, "list folder %s: %v", entry.FullPath, err)
				return fmt.Errorf("list folder %s: %v", entry.FullPath, err)
			}
			if lastFileName == "" && !isRecursive && len(entries) > 0 {
				// only for first iteration in the loop
				glog.V(2).InfofCtx(ctx, "deleting a folder %s has children: %+v ...", entry.FullPath, entries[0].Name())
				return fmt.Errorf("%w: %s", ErrNonEmptyFolder, entry.FullPath)
			}

			// Refuse before any side effect of this page (events, remote
			// deletes, the folder-children sweep): a locked entry anywhere in
			// it fails the whole delete rather than slipping through the
			// store-level drop the walk ends in. Refusal is not a recursive
			// error -- ignoreRecursiveError must not waive a lock.
			if isRecursive && !isFromOtherCluster {
				for _, sub := range entries {
					if lockErr := entryDeleteLockError(ctx, sub); lockErr != nil {
						f.unnoteRemoteDeletion(entry.FullPath, dirTombstoneTs)
						return lockErr
					}
				}
			}

			for _, sub := range entries {
				lastFileName = sub.Name()
				if !keepsRemoteObject(ctx) {
					f.noteRemoteDeletion(sub.FullPath, sub.IsDirectory(), time.Now().UnixNano())
				}
				if sub.IsDirectory() {
					subIsDeletingBucket := f.IsBucket(sub)
					err = f.doBatchDeleteFolderMetaAndData(ctx, sub, isRecursive, ignoreRecursiveError, shouldDeleteChunks, subIsDeletingBucket, isFromOtherCluster, nil, onHardLinkIdsFn)
				} else {
					if !isFromOtherCluster && !keepsRemoteObject(ctx) {
						if _, remoteErr := f.maybeDeleteFromRemote(ctx, sub); remoteErr != nil {
							glog.Warningf("remote delete child %s: %v", sub.FullPath, remoteErr)
							if !ignoreRecursiveError {
								err = remoteErr
							}
						}
					}
					if err != nil && (!ignoreRecursiveError || errors.Is(err, ErrEntryLocked)) {
						break
					}
					f.NotifyUpdateEvent(ctx, deleteEventEntry(ctx, sub), nil, shouldDeleteChunks, isFromOtherCluster, nil)
					if len(sub.HardLinkId) != 0 {
						// hard link chunk data are deleted separately
						err = onHardLinkIdsFn([]HardLinkId{sub.HardLinkId})
						if shouldDeleteChunks && f.hardLinkIsOrphaned(ctx, sub.HardLinkId) {
							chunksToDelete = append(chunksToDelete, sub.GetChunks()...)
						}
					} else {
						if shouldDeleteChunks {
							chunksToDelete = append(chunksToDelete, sub.GetChunks()...)
						}
					}
				}
				if err != nil && (!ignoreRecursiveError || errors.Is(err, ErrEntryLocked)) {
					return err
				}
			}

			if len(entries) < PaginationSize {
				break
			}
		}
	}

	glog.V(3).InfofCtx(ctx, "deleting directory %v delete chunks: %v", entry.FullPath, shouldDeleteChunks)

	// a non-recursive delete already proved the folder empty above, so sweeping the
	// children now can only remove entries that raced in after that listing
	if isRecursive || !listedChildren {
		if storeDeletionErr := f.Store.DeleteFolderChildren(ctx, entry.FullPath); storeDeletionErr != nil {
			return fmt.Errorf("filer store delete: %w", storeDeletionErr)
		}
	}

	f.NotifyUpdateEvent(ctx, deleteEventEntry(ctx, entry), nil, shouldDeleteChunks, isFromOtherCluster, signatures)
	f.DeleteChunks(ctx, entry.FullPath, chunksToDelete)

	return nil
}

func (f *Filer) doDeleteEntryMetaAndData(ctx context.Context, entry *Entry, shouldDeleteChunks bool, isFromOtherCluster bool, signatures []int32) (err error) {

	glog.V(3).InfofCtx(ctx, "deleting entry %v, delete chunks: %v", entry.FullPath, shouldDeleteChunks)

	if !isFromOtherCluster {
		if lockErr := entryDeleteLockError(ctx, entry); lockErr != nil {
			return lockErr
		}
	}

	if !isFromOtherCluster && !keepsRemoteObject(ctx) {
		if _, remoteDeletionErr := f.maybeDeleteFromRemote(ctx, entry); remoteDeletionErr != nil {
			return remoteDeletionErr
		}
	}

	// A metadata-only delete owes the remote nothing, so lazy reads stay open.
	if !keepsRemoteObject(ctx) {
		f.noteRemoteDeletion(entry.FullPath, entry.IsDirectory(), time.Now().UnixNano())
	}

	if storeDeletionErr := f.Store.DeleteOneEntry(ctx, entry); storeDeletionErr != nil {
		return fmt.Errorf("filer store delete: %w", storeDeletionErr)
	}

	if !entry.IsDirectory() {
		f.NotifyUpdateEvent(ctx, deleteEventEntry(ctx, entry), nil, shouldDeleteChunks, isFromOtherCluster, signatures)
	}

	return nil
}

// collectionDeleteTimeout bounds the collection delete a bucket entry's own
// delete leaves behind, which carries no deadline of its own. It bounds the wait
// and not the work: the master keeps deleting on its own fan-out once asked, so
// giving up costs the confirmation. Short enough that the S3 client waiting on
// the bucket delete, which pays this and then the gateway's own follow-up
// DeleteCollection, still has retry budget left.
const collectionDeleteTimeout = 15 * time.Second

// bucketCollection resolves the collection a bucket's objects land in
// through the same chain the write path uses -- a grouped gateway's explicit
// collection, then the storage rules, then the bucket name -- and reports it
// only when nothing outside the bucket can still route into it. A shared
// collection must survive the bucket delete: dropping it removes volumes
// other paths still write to. A listing failure keeps the collection, the
// safe side of an unknown.
func (f *Filer) bucketCollection(ctx context.Context, bucket string) (collection string) {
	bucketDir := f.DirBucketsPath + "/" + bucket + "/"
	resolve := func(dir, name string) string {
		if f.MasterClient != nil {
			if group := f.MasterClient.FilerGroup; group != "" {
				return group + "_" + name
			}
		}
		return util.Nvl(f.FilerConf.MatchStorageRule(dir).Collection, name)
	}
	collection = resolve(bucketDir, bucket)

	// Rule-less writes outside buckets fall back to the filer's default
	// collection, so a bucket resolving there shares it with them. The
	// system metadata-log collection (when explicitly redirected via
	// filer.options.metaLog.collection) is in the same boat: it backs internal
	// log volumes, so a bucket that resolves there must never drop it
	// either.
	if collection == f.metaLogCollection {
		return ""
	}
	if f.metaLogTargetCollection != "" && collection == f.metaLogTargetCollection {
		return ""
	}

	// A rule whose prefix escapes the bucket can route other paths into the
	// same collection, including prefixes nested under surviving buckets.
	for _, rule := range f.FilerConf.ToProto().Locations {
		prefix := strings.TrimSuffix(rule.LocationPrefix, "/") + "/"
		if strings.HasPrefix(prefix, bucketDir) {
			continue
		}
		if f.FilerConf.MatchStorageRule(prefix).Collection == collection {
			return ""
		}
	}

	siblings, err := f.listBuckets(ctx)
	if err != nil {
		glog.ErrorfCtx(ctx, "list buckets for collection check: %v", err)
		return ""
	}
	for _, sibling := range siblings {
		if sibling != bucket && resolve(f.DirBucketsPath+"/"+sibling+"/", sibling) == collection {
			return ""
		}
	}
	return collection
}

func (f *Filer) listBuckets(ctx context.Context) (buckets []string, err error) {
	lastFileName := ""
	for {
		entries, _, listErr := f.ListDirectoryEntries(ctx, util.FullPath(f.DirBucketsPath), lastFileName, false, PaginationSize, "", "", "")
		if listErr != nil {
			return nil, listErr
		}
		for _, entry := range entries {
			lastFileName = entry.Name()
			if f.IsBucket(entry) {
				buckets = append(buckets, entry.Name())
			}
		}
		if len(entries) < PaginationSize {
			return buckets, nil
		}
	}
}

func (f *Filer) DoDeleteCollection(ctx context.Context, collectionName string) (err error) {

	return f.MasterClient.WithClient(ctx, false, func(client master_pb.SeaweedClient) error {
		_, err := client.CollectionDelete(ctx, &master_pb.CollectionDeleteRequest{
			Name: collectionName,
		})
		if err != nil {
			glog.Infof("delete collection %s: %v", collectionName, err)
		}
		return err
	})

}

func (f *Filer) maybeDeleteHardLinks(ctx context.Context, hardLinkIds []HardLinkId) {
	for _, hardLinkId := range hardLinkIds {
		if err := f.Store.DeleteHardLink(ctx, hardLinkId); err != nil {
			glog.ErrorfCtx(ctx, "delete hard link id %d : %v", hardLinkId, err)
		}
	}
}

// hardLinkIsOrphaned reports whether the shared inode record for hardLinkId
// is gone, meaning the removed link was the last owner of its chunks.
func (f *Filer) hardLinkIsOrphaned(ctx context.Context, hardLinkId HardLinkId) bool {
	_, err := f.Store.KvGet(ctx, hardLinkId)
	return errors.Is(err, ErrKvNotFound)
}
