package filer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type lazyFetchContextKey struct{}

// maybeLazyFetchFromRemote is called by FindEntry when the store returns no
// entry for p. If p is under a remote-storage mount, it stats the remote
// object, builds a filer Entry from the result, and persists it via
// CreateEntry with SkipCheckParentDirectory so phantom parent directories
// under the mount are not required.
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

	client, _, found := f.RemoteStorage.FindRemoteStorageClient(p)
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
			Remote: remoteEntry,
		}

		persistBaseCtx, cancelPersist := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelPersist()
		persistCtx := context.WithValue(persistBaseCtx, lazyFetchContextKey{}, true)
		saveErr := f.CreateEntry(persistCtx, entry, false, false, nil, true, f.MaxFilenameLength)
		if saveErr != nil {
			glog.Warningf("maybeLazyFetchFromRemote: failed to persist filer entry for %s: %v", p, saveErr)
			f.lazyFetchGroup.Forget(key)
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
