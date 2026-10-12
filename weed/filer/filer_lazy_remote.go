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
		f.fetchMissingRemoteParents(persistBaseCtx, mountDir, p)
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
	for i := len(missing) - 1; i >= 0; i-- {
		f.FindEntry(ctx, missing[i])
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

func (f *Filer) maybeDeleteFromRemote(ctx context.Context, entry *Entry) (bool, error) {
	if entry == nil || f.RemoteStorage == nil {
		return false, nil
	}

	mountDir, remoteLoc := f.RemoteStorage.FindMountDirectory(entry.FullPath)
	if remoteLoc == nil {
		return false, nil
	}

	if !entry.IsDirectory() && entry.Remote == nil {
		return false, nil
	}

	remoteConf, found := f.RemoteStorage.GetRemoteStorageConf(remoteLoc.Name)
	if !found {
		return false, fmt.Errorf("resolve remote storage client for %s: not found", entry.FullPath)
	}
	client, clientErr := f.buildRemoteStorageClient(ctx, remoteConf)
	if clientErr != nil {
		return false, fmt.Errorf("resolve remote storage client for %s: %w", entry.FullPath, clientErr)
	}
	if client == nil {
		return false, fmt.Errorf("resolve remote storage client for %s: initialization failed", entry.FullPath)
	}

	objectLoc := MapFullPathToRemoteStorageLocation(mountDir, remoteLoc, entry.FullPath)

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
