package filer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type lazyFetchContextKey struct{}

// lazyParentsContextKey marks a FindEntry issued while materializing missing
// parents: that fetch's own parent walk would only re-stat the same gaps.
type lazyParentsContextKey struct{}

// maybeLazyFetchFromRemote is called by FindEntry when the store returns no
// entry for p. If p is under a remote-storage mount, it stats the remote
// object, builds a filer Entry from the result, and persists it via
// CreateEntry together with its missing parent directories: remote.uncache
// and cache eviction find entries by walking down from the mount directory.
//
// On a CreateEntry failure after a successful StatFile the in-memory entry is
// still returned (availability over consistency); the singleflight key is
// forgotten so the next lookup retries the filer write.
//
// Returns nil without error when: p is not under a remote mount; the remote
// reports the object does not exist; or any other remote error occurs.
func (f *Filer) maybeLazyFetchFromRemote(ctx context.Context, p util.FullPath) (*Entry, error) {
	// Prevent recursive invocation: CreateEntry calls FindEntry, which would
	// re-enter this function and deadlock on the singleflight key.
	if ctx.Value(lazyFetchContextKey{}) != nil {
		return nil, nil
	}

	if f.RemoteStorage == nil {
		return nil, nil
	}

	mountDir, remoteLoc := f.RemoteStorage.FindMountDirectory(p)
	if remoteLoc == nil {
		return nil, nil
	}

	// A startup tombstone rebuild may still be replaying the meta log; wait
	// for it so a pending delete cannot resurrect here.
	if done := f.remoteTombstonesDone.Load(); done != nil {
		select {
		case <-*done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if f.isRemoteDeletionPending(ctx, p, mountDir) {
		glog.V(2).InfofCtx(ctx, "maybeLazyFetchFromRemote: %s deleted locally, remote delete pending", p)
		return nil, nil
	}

	remoteConf, found := f.RemoteStorage.FindRemoteStorageConf(p)
	if !found {
		return nil, nil
	}

	relPath := strings.TrimPrefix(string(p), string(mountDir))
	if relPath != "" && !strings.HasPrefix(relPath, "/") {
		relPath = "/" + relPath
	}
	base := strings.TrimSuffix(remoteLoc.Path, "/")
	remotePath := "/" + strings.TrimLeft(base+relPath, "/")

	objectLoc := &remote_pb.RemoteStorageLocation{
		Name:   remoteLoc.Name,
		Bucket: remoteLoc.Bucket,
		Path:   remotePath,
	}

	type lazyFetchResult struct {
		entry *Entry
	}

	key := string(p)
	val, err, _ := f.lazyFetchGroup.Do(key, func() (interface{}, error) {
		buildCtx := context.WithoutCancel(ctx)
		client, clientErr := f.buildRemoteStorageClient(buildCtx, remoteConf)
		if clientErr != nil {
			glog.V(1).InfofCtx(ctx, "maybeLazyFetchFromRemote: reject %s: %v", p, clientErr)
			return lazyFetchResult{nil}, nil
		}
		remoteEntry, statErr := client.StatFile(objectLoc)
		if statErr != nil {
			if errors.Is(statErr, remote_storage.ErrRemoteObjectNotFound) {
				glog.V(3).InfofCtx(ctx, "maybeLazyFetchFromRemote: %s not found in remote", p)
			} else {
				glog.Warningf("maybeLazyFetchFromRemote: stat %s failed: %v", p, statErr)
			}
			return lazyFetchResult{nil}, nil
		}
		if remoteEntry == nil {
			glog.V(3).InfofCtx(ctx, "maybeLazyFetchFromRemote: %s StatFile returned nil entry", p)
			return lazyFetchResult{nil}, nil
		}

		mtime := time.Unix(remoteEntry.RemoteMtime, 0)
		entry := &Entry{
			FullPath: p,
			Attr: Attr{
				Mtime:    mtime,
				Crtime:   mtime,
				Mode:     0644,
				FileSize: uint64(remoteEntry.RemoteSize),
			},
			Extended: MergeRemoteContentEncoding(remoteEntry, nil),
			Remote:   remoteEntry,
		}

		persistBaseCtx, cancelPersist := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelPersist()
		persistCtx := context.WithValue(persistBaseCtx, lazyFetchContextKey{}, true)
		// A delete may have landed while StatFile was in flight; re-check so
		// the fetched object cannot resurrect a path whose delete is pending.
		if f.isRemoteDeletionPending(persistCtx, p, mountDir) {
			glog.V(2).InfofCtx(ctx, "maybeLazyFetchFromRemote: %s deleted during remote stat", p)
			return lazyFetchResult{nil}, nil
		}
		if ctx.Value(lazyParentsContextKey{}) == nil {
			f.fetchMissingRemoteParents(persistBaseCtx, mountDir, p)
		}
		saveErr := f.CreateEntry(persistCtx, entry, nil, false, false, nil, false, f.MaxFilenameLength)
		if saveErr != nil {
			glog.Warningf("maybeLazyFetchFromRemote: failed to persist filer entry for %s: %v", p, saveErr)
			f.lazyFetchGroup.Forget(key)
			return lazyFetchResult{entry}, nil
		}

		// A delete records its tombstone before removing the entry, so a
		// tombstone visible now means the insert raced a delete that already
		// ran: retract the persisted entry so the path stays deleted.
		if f.isRemoteDeletionPending(persistCtx, p, mountDir) {
			glog.V(2).InfofCtx(ctx, "maybeLazyFetchFromRemote: %s deleted while persisting", p)
			f.lazyFetchGroup.Forget(key)
			f.retractLazyRemoteEntry(persistCtx, entry)
			return lazyFetchResult{nil}, nil
		}

		return lazyFetchResult{entry}, nil
	})
	if err != nil {
		return nil, err
	}

	result, ok := val.(lazyFetchResult)
	if !ok {
		return nil, fmt.Errorf("maybeLazyFetchFromRemote: unexpected singleflight result type %T for %s", val, p)
	}
	return result.entry, nil
}

// fetchMissingRemoteParents looks up p's missing parents under mountDir, top
// down, through lazy fetch. A remote object stored at a parent's key is then
// materialized first, so creating p promotes it to a prefix object instead of
// hiding it behind a plain directory. Each missing parent costs one remote
// stat, once per directory.
func (f *Filer) fetchMissingRemoteParents(ctx context.Context, mountDir, p util.FullPath) {
	var missing []util.FullPath
	for dir, _ := p.DirAndName(); len(dir) > len(mountDir); dir, _ = util.FullPath(dir).DirAndName() {
		if _, err := f.Store.FindEntry(ctx, util.FullPath(dir)); !errors.Is(err, filer_pb.ErrNotFound) {
			break
		}
		missing = append(missing, util.FullPath(dir))
	}
	parentCtx := context.WithValue(ctx, lazyParentsContextKey{}, true)
	for i := len(missing) - 1; i >= 0; i-- {
		f.FindEntry(parentCtx, missing[i])
	}
}

// retractLazyRemoteEntry deletes the entry at entry.FullPath only when it is
// still the entry a lazy remote read just materialized — a concurrent write
// may have replaced it, and deleting by path alone would take that write down.
func (f *Filer) retractLazyRemoteEntry(ctx context.Context, entry *Entry) {
	existing, findErr := f.FindEntry(ctx, entry.FullPath)
	if findErr != nil || existing == nil {
		return
	}
	// The stored entry must still be exactly what this read materialized —
	// an intervening write (appended chunks, touched attributes) means a
	// real update owns the path now.
	if !proto.Equal(existing.ToProtoEntry(), entry.ToProtoEntry()) {
		return
	}
	if err := f.doDeleteEntryMetaAndData(ctx, existing, false, false, nil); err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		glog.Warningf("retractLazyRemoteEntry %s: %v", entry.FullPath, err)
	}
}

// remoteStorageObject resolves the client and object location entry maps to
// on its mounted remote storage; a nil client means the path is not mounted.
func (f *Filer) remoteStorageObject(ctx context.Context, entry *Entry) (remote_storage.RemoteStorageClient, *remote_pb.RemoteStorageLocation, error) {
	mountDir, remoteLoc := f.RemoteStorage.FindMountDirectory(entry.FullPath)
	if remoteLoc == nil {
		return nil, nil, nil
	}

	remoteConf, found := f.RemoteStorage.GetRemoteStorageConf(remoteLoc.Name)
	if !found {
		return nil, nil, fmt.Errorf("resolve remote storage client for %s: not found", entry.FullPath)
	}
	client, clientErr := f.buildRemoteStorageClient(ctx, remoteConf)
	if clientErr != nil {
		return nil, nil, fmt.Errorf("resolve remote storage client for %s: %w", entry.FullPath, clientErr)
	}
	if client == nil {
		return nil, nil, fmt.Errorf("resolve remote storage client for %s: initialization failed", entry.FullPath)
	}

	return client, MapFullPathToRemoteStorageLocation(mountDir, remoteLoc, entry.FullPath), nil
}

func (f *Filer) maybeDeleteFromRemote(ctx context.Context, entry *Entry) (bool, error) {
	if entry == nil || f.RemoteStorage == nil {
		return false, nil
	}

	if !entry.IsDirectory() && entry.Remote == nil {
		return false, nil
	}

	client, objectLoc, err := f.remoteStorageObject(ctx, entry)
	if err != nil || client == nil {
		return false, err
	}

	if entry.IsDirectory() {
		if err := client.RemoveDirectory(objectLoc); err != nil {
			if errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
				return true, nil
			}
			return false, fmt.Errorf("remove remote directory %s: %w", entry.FullPath, err)
		}
		glog.V(3).InfofCtx(ctx, "maybeDeleteFromRemote: deleted directory %s from remote", entry.FullPath)
		return true, nil
	}

	if err := client.DeleteFile(objectLoc); err != nil {
		if errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
			return true, nil
		}
		return false, fmt.Errorf("delete remote file %s: %w", entry.FullPath, err)
	}

	glog.V(3).InfofCtx(ctx, "maybeDeleteFromRemote: deleted %s from remote", entry.FullPath)
	return true, nil
}

// deleteSweptEntryRemote frees the remote objects a swept entry still owns,
// mirroring the write-back daemon's delete guards (processDeleteEvent): the
// source a pending rename marker still reads goes first — nothing else
// releases it — then the object at the entry's own key, each only while it
// is still the object the entry's stamp describes.
func (f *Filer) deleteSweptEntryRemote(ctx context.Context, entry *Entry) error {
	if entry == nil || f.RemoteStorage == nil || entry.Remote == nil {
		return nil
	}
	client, objectLoc, err := f.remoteStorageObject(ctx, entry)
	if err != nil || client == nil {
		return err
	}
	if err := f.releasePendingRenameSource(ctx, client, entry); err != nil {
		return err
	}
	return deleteRemoteFileIfUnchanged(ctx, client, objectLoc, entry.Remote)
}

// releasePendingRenameSource deletes the object a pending-rename marker on
// entry still reads: the destination copy never landed, so the source key is
// the content's only location and dropping the entry would orphan it. The
// source survives while a remote-only entry occupies its path — that entry's
// content is the same object. The delete binds to entry's stamp, which is
// the stamp of the source the marker reads. Mirrors the write-back daemon's
// releasePendingSource.
func (f *Filer) releasePendingRenameSource(ctx context.Context, client remote_storage.RemoteStorageClient, entry *Entry) error {
	src := f.RemoteSourcePath(entry, entry.FullPath)
	if src == entry.FullPath {
		return nil
	}
	srcEntry, err := f.Store.FindEntry(ctx, src)
	if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		return fmt.Errorf("find rename source %s: %w", src, err)
	}
	if srcEntry != nil && len(srcEntry.Content) == 0 && srcEntry.IsInRemoteOnly() {
		glog.V(0).InfofCtx(ctx, "keep remote source %s of pending rename of %s: a remote-only entry holds its path", src, entry.FullPath)
		return nil
	}
	mountDir, remoteLoc := f.RemoteStorage.FindMountDirectory(src)
	if remoteLoc == nil {
		return nil
	}
	return deleteRemoteFileIfUnchanged(ctx, client, MapFullPathToRemoteStorageLocation(mountDir, remoteLoc, src), entry.Remote)
}

// deleteRemoteFileIfUnchanged deletes the remote object only while it is
// still the one stamp describes. Backends offering a conditional delete bind
// the check and the delete into one preconditioned request; the rest fall
// back to a guarded stat-then-delete.
func deleteRemoteFileIfUnchanged(ctx context.Context, client remote_storage.RemoteStorageClient, objectLoc *remote_pb.RemoteStorageLocation, stamp *filer_pb.RemoteEntry) error {
	if deleter, ok := client.(remote_storage.RemoteStorageConditionalDeleter); ok {
		deleted, delErr := deleter.DeleteFileIfUnchanged(objectLoc, stamp)
		if delErr != nil {
			return fmt.Errorf("delete remote file %s: %w", remote_storage.FormatLocation(objectLoc), delErr)
		}
		if !deleted {
			glog.V(0).InfofCtx(ctx, "keep %s: the remote object changed since the delete", remote_storage.FormatLocation(objectLoc))
		}
		return nil
	}
	live, statErr := client.StatFile(objectLoc)
	if statErr != nil {
		if errors.Is(statErr, remote_storage.ErrRemoteObjectNotFound) {
			return nil
		}
		return fmt.Errorf("stat remote file %s: %w", remote_storage.FormatLocation(objectLoc), statErr)
	}
	if !remote_storage.StillCurrent(stamp, live) {
		glog.V(0).InfofCtx(ctx, "keep %s: the remote object changed since the delete", remote_storage.FormatLocation(objectLoc))
		return nil
	}
	if err := client.DeleteFile(objectLoc); err != nil && !errors.Is(err, remote_storage.ErrRemoteObjectNotFound) {
		return fmt.Errorf("delete remote file %s: %w", remote_storage.FormatLocation(objectLoc), err)
	}
	return nil
}

// DeleteFromRemote deletes the object, or for a directory every object, the
// entry maps to on its mounted remote storage.
func (f *Filer) DeleteFromRemote(ctx context.Context, entry *Entry) error {
	_, err := f.maybeDeleteFromRemote(ctx, entry)
	return err
}

// ExtRemoteRenameSourceKey marks a remote-only entry whose rename committed
// while its copy to the new key was still pending: it holds the path whose
// object remote-only reads must serve until the copy stamps the entry.
const ExtRemoteRenameSourceKey = "Seaweed-X-Remote-Rename-Source"

// RemoteSourcePath returns the path whose remote object holds the content of
// the remote-only entry at p: the renamed-from path a pending deferred copy
// still reads, or p itself. A marker leading out of p's mount is ignored, so
// whoever can set extended attributes cannot point reads at another mount.
func (f *Filer) RemoteSourcePath(entry *Entry, p util.FullPath) util.FullPath {
	src := util.FullPath(entry.Extended[ExtRemoteRenameSourceKey])
	if src == "" || f.RemoteStorage == nil {
		return p
	}
	mountDir, remoteLoc := f.RemoteStorage.FindMountDirectory(p)
	if remoteLoc == nil {
		return p
	}
	if srcMountDir, _ := f.RemoteStorage.FindMountDirectory(src); srcMountDir != mountDir {
		return p
	}
	return src
}

// RemoteReadPath returns the dir and name whose object a remote-only read of
// the entry at dir/name serves.
func (f *Filer) RemoteReadPath(entry *Entry, dir, name string) (string, string) {
	return f.RemoteSourcePath(entry, util.NewFullPath(dir, name)).DirAndName()
}

// CopyRemoteOnlyEntry copies the remote object of a renamed entry whose
// content exists only on the remote to the key of newPath, and returns the
// RemoteEntry of the copy. The rename deletes the old key along with the old
// entry, and with no local data nothing would write the new key. It returns
// nil when no copy is needed: the entry has local data, which the remote sync
// uploads to the new key, it is empty, which the sync writes as an empty
// object, or it is not under a remote mount. A rename that
// moves such an entry out of its mount, or onto a remote that cannot copy, is
// refused, since it would delete the only copy of the content.
func (f *Filer) CopyRemoteOnlyEntry(ctx context.Context, entry *Entry, oldPath, newPath util.FullPath) (*filer_pb.RemoteEntry, error) {
	src, dst, copier, err := f.RemoteCopyLocations(ctx, entry, oldPath, newPath)
	if err != nil || copier == nil {
		return nil, err
	}
	remoteEntry, err := copier.CopyFile(src, dst)
	if err != nil {
		return nil, fmt.Errorf("copy remote %s to %s: %w", remote_storage.FormatLocation(src), remote_storage.FormatLocation(dst), err)
	}
	glog.V(3).InfofCtx(ctx, "CopyRemoteOnlyEntry: copied %s to %s", remote_storage.FormatLocation(src), remote_storage.FormatLocation(dst))
	return remoteEntry, nil
}

// RemoteCopyLocations resolves the object locations a remote-only rename copy
// of entry between oldPath and newPath would use, and the client able to copy
// it. A nil copier means the entry needs no copy; an error means it needs one
// that cannot be served, which a rename must refuse before committing.
func (f *Filer) RemoteCopyLocations(ctx context.Context, entry *Entry, oldPath, newPath util.FullPath) (src, dst *remote_pb.RemoteStorageLocation, copier remote_storage.RemoteStorageObjectCopier, err error) {
	if f.RemoteStorage == nil || entry.IsDirectory() || !entry.IsInRemoteOnly() || len(entry.Content) > 0 {
		return nil, nil, nil, nil
	}

	mountDir, remoteLoc := f.RemoteStorage.FindMountDirectory(oldPath)
	if remoteLoc == nil {
		return nil, nil, nil, nil
	}
	if newMountDir, _ := f.RemoteStorage.FindMountDirectory(newPath); newMountDir != mountDir {
		return nil, nil, nil, fmt.Errorf("%s exists only on the remote of mount %s and cannot be renamed out of it; cache it first", oldPath, mountDir)
	}

	remoteConf, found := f.RemoteStorage.GetRemoteStorageConf(remoteLoc.Name)
	if !found {
		return nil, nil, nil, fmt.Errorf("resolve remote storage client for %s: not found", oldPath)
	}
	client, clientErr := f.buildRemoteStorageClient(ctx, remoteConf)
	if clientErr != nil {
		return nil, nil, nil, fmt.Errorf("resolve remote storage client for %s: %w", oldPath, clientErr)
	}
	copier, ok := client.(remote_storage.RemoteStorageObjectCopier)
	if !ok {
		return nil, nil, nil, fmt.Errorf("%s exists only on the remote and remote storage type %s cannot copy it to the new name; cache it first", oldPath, remoteConf.Type)
	}

	src = MapFullPathToRemoteStorageLocation(mountDir, remoteLoc, f.RemoteSourcePath(entry, oldPath))
	dst = MapFullPathToRemoteStorageLocation(mountDir, remoteLoc, newPath)
	return src, dst, copier, nil
}
