package storage

import (
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
)

func (v *Volume) GetVolumeInfo() *volume_server_pb.VolumeInfo {
	return v.volumeInfo
}

func (v *Volume) maybeLoadVolumeInfo() (found bool) {

	var err error
	var hasRemoteFile bool
	v.volumeInfo, hasRemoteFile, found, err = volume_info.MaybeLoadVolumeInfo(v.FileName(".vif"))
	v.hasRemoteFile.Store(hasRemoteFile)
	internVolumeInfoStrings(v.volumeInfo)

	if v.volumeInfo.Version == 0 {
		v.volumeInfo.Version = uint32(needle.GetCurrentVersion())
	}

	if hasRemoteFile {
		glog.V(0).Infof("volume %d is tiered to %s as %s and read only", v.Id,
			v.volumeInfo.Files[0].BackendName(), v.volumeInfo.Files[0].Key)
	} else {
		if v.volumeInfo.BytesOffset == 0 {
			v.volumeInfo.BytesOffset = uint32(types.OffsetSize)
		}
	}

	if v.volumeInfo.BytesOffset != 0 && v.volumeInfo.BytesOffset != uint32(types.OffsetSize) {
		var m string
		if types.OffsetSize == 5 {
			m = "without"
		} else {
			m = "with"
		}
		glog.Exitf("BytesOffset mismatch in volume info file %s, try use binary version %s large_disk", v.FileName(".vif"), m)
		return
	}

	if err != nil {
		glog.Warningf("load volume %d.vif file: %v", v.Id, err)
		return
	}

	return

}

// internVolumeInfoStrings shares the values every volume's .vif repeats. A
// tiered volume names its replication and its backend on every load, and the
// decode allocates a fresh copy of each, so a server holding millions of them
// otherwise holds millions of copies of the same handful of names. The remote
// key is left alone: it names one volume.
func internVolumeInfoStrings(volumeInfo *volume_server_pb.VolumeInfo) {
	volumeInfo.Replication = internVolumeString(volumeInfo.Replication)
	for _, remoteFile := range volumeInfo.GetFiles() {
		remoteFile.BackendType = internVolumeString(remoteFile.BackendType)
		remoteFile.BackendId = internVolumeString(remoteFile.BackendId)
		remoteFile.Extension = internVolumeString(remoteFile.Extension)
	}
}

func (v *Volume) HasRemoteFile() bool {
	return v.hasRemoteFile.Load()
}

// LoadRemoteFile swaps the data backend to the remote tier object under
// dataFileAccessLock. Call this from a context that does NOT already hold the
// lock — the live tier-upload handler, where the heartbeat may be reading the
// backend concurrently. load() must instead use loadRemoteFileLocked, since it
// can be reached with the lock already held (CommitCompact).
func (v *Volume) LoadRemoteFile() error {
	v.dataFileSyncLock.Lock()
	defer v.dataFileSyncLock.Unlock()
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	return v.loadRemoteFileLocked()
}

// MarkTieredDeletable gives a volume whose data has just moved to the remote
// tier the flags a restart gives it (load(): a tiered volume refuses writes and
// accepts deletes). volume.tier.upload marks the volume read-only without
// deletes before the upload; left that way, every delete until the next
// restart is refused, the filer drops it, and the needle's bytes stay in the
// remote object where no vacuum can find them.
//
// Only for a single-copy volume: the shell copies the uploaded replica's .idx
// and .vif to the other replicas after the upload, and a tombstone written
// before that copy would reach one replica only; a replicated volume keeps
// refusing deletes until its next load, as before. An I/O quarantine is left
// alone, and so is a volume past the I/O error tolerance that the next
// heartbeat will quarantine. Otherwise the flags are the ones load() gives
// every tiered volume.
func (v *Volume) MarkTieredDeletable() bool {
	if rp := v.SuperBlock.ReplicaPlacement; rp != nil && rp.GetCopyCount() > 1 {
		return false
	}
	// Check the I/O state under noWriteLock: CollectHeartbeat marks the
	// quarantine before it takes noWriteLock to refuse deletes, so either the
	// check below sees the quarantine, or the heartbeat's refusal lands after
	// this change and stays.
	v.noWriteLock.Lock()
	defer v.noWriteLock.Unlock()
	if !v.HasRemoteFile() {
		return false
	}
	if ioErr, ioCount, quarantined := v.getIoErrorState(); quarantined || (ioErr != nil && ioCount >= IoErrorTolerance) {
		return false
	}
	v.noWriteOrDelete = false
	v.noWriteCanDelete = true
	return true
}

// loadRemoteFileLocked swaps the data backend to the remote tier object. The
// caller must hold dataFileAccessLock or be single-threaded (load() during
// construction or a compaction-commit reload). It marks the volume tiered in the
// same locked step so a later heartbeat does not treat a removed local .dat as a
// phantom volume and stop reporting it to the master.
func (v *Volume) loadRemoteFileLocked() error {
	// Callers only reach here for a tiered volume (HasRemoteFile / a just-appended
	// remote file), but guard the index so a stray call is a clean error, not a panic.
	if len(v.volumeInfo.GetFiles()) == 0 {
		return fmt.Errorf("volume %d has no remote file to load", v.Id)
	}
	tierFile := v.volumeInfo.GetFiles()[0]
	backendStorage, found := backend.BackendStorages[tierFile.BackendName()]
	if !found {
		return fmt.Errorf("backend storage %s not found", tierFile.BackendName())
	}
	v.swapDataBackendLocked(backendStorage.NewStorageFile(tierFile.Key, v.volumeInfo), true)
	return nil
}

// AttachRemoteDatFile records remoteFile, a copy of the .dat read from source,
// in the volume's .vif and switches the data backend to it. It refuses when
// source is no longer the volume's data backend: a compaction commit since the
// copy replaced the .dat and the index, and the copy has the old layout. The
// check, the .vif write and the switch happen under the data-file locks, so a
// commit either lands before (and is seen here) or after (and refuses a tiered
// volume).
func (v *Volume) AttachRemoteDatFile(source backend.BackendStorageFile, remoteFile *volume_server_pb.RemoteFile) error {
	v.dataFileSyncLock.Lock()
	defer v.dataFileSyncLock.Unlock()
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	if v.DataBackend != source {
		return fmt.Errorf("volume %d data file changed during the copy (compacted or reloaded)", v.Id)
	}
	files := v.volumeInfo.GetFiles()
	v.volumeInfo.Files = append(files, remoteFile)
	if err := v.SaveVolumeInfo(); err != nil {
		// A NotCrashDurableError means the rename committed: the .vif names
		// the copy, so keep the entry and serve from the copy, and still
		// report the error so the caller keeps the local .dat. Any other
		// failure left the old .vif in place.
		var ndErr *volume_info.NotCrashDurableError
		if !errors.As(err, &ndErr) {
			v.volumeInfo.Files = files
			return fmt.Errorf("save remote file info: %w", err)
		}
		if loadErr := v.loadRemoteFileLocked(); loadErr != nil {
			return fmt.Errorf("save remote file info: %w; load remote file: %v", err, loadErr)
		}
		return fmt.Errorf("save remote file info: %w", err)
	}
	return v.loadRemoteFileLocked()
}

func (v *Volume) SaveVolumeInfo() error {

	tierFileName := v.FileName(".vif")
	if expireAtSec := v.ExpireAtSec(); expireAtSec > 0 {
		v.volumeInfo.ExpireAtSec = expireAtSec
	}

	return saveVolumeInfoFile(tierFileName, v.volumeInfo)

}

// saveVolumeInfoFile writes a .vif; tests replace it to inject failures.
var saveVolumeInfoFile = volume_info.SaveVolumeInfo
