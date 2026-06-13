package weed_server

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeTierMoveDatFromRemote copy dat file from a remote tier to local volume server
func (vs *VolumeServer) VolumeTierMoveDatFromRemote(req *volume_server_pb.VolumeTierMoveDatFromRemoteRequest, stream volume_server_pb.VolumeServer_VolumeTierMoveDatFromRemoteServer) error {

	// find existing volume
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("volume %d not found", req.VolumeId)
	}

	// verify the collection
	if v.Collection != req.Collection {
		return fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	// locate the disk file
	storageName, storageKey := v.RemoteStorageNameKey()
	if storageName == "" || storageKey == "" {
		return fmt.Errorf("volume %d is already on local disk", req.VolumeId)
	}

	// check whether the local .dat already exists
	_, ok := v.DataBackend.(*backend.DiskFile)
	if ok {
		return fmt.Errorf("volume %d is already on local disk", req.VolumeId)
	}

	// check valid storage backend type
	backendStorage, found := backend.BackendStorages[storageName]
	if !found {
		var keys []string
		for key := range backend.BackendStorages {
			keys = append(keys, key)
		}
		return fmt.Errorf("remote storage %s not found from supported: %v", storageName, keys)
	}

	startTime := time.Now()
	fn := func(progressed int64, percentage float32) error {
		now := time.Now()
		if now.Sub(startTime) < time.Second {
			return nil
		}
		startTime = now
		return stream.Send(&volume_server_pb.VolumeTierMoveDatFromRemoteResponse{
			Processed:           progressed,
			ProcessedPercentage: percentage,
		})
	}
	// copy the data file (DownloadFile opens, fsyncs, and closes the .dat internally)
	datFileName := v.FileName(".dat")
	_, err := backendStorage.DownloadFile(datFileName, storageKey, fn)
	if err != nil {
		return fmt.Errorf("backend %s copy file %s: %v", storageName, datFileName, err)
	}

	// fsync the containing directory so the new .dat and the about-to-be-rewritten
	// .vif are durably linked before we touch the shared remote object.
	if err := fsyncDir(filepath.Dir(datFileName)); err != nil {
		return fmt.Errorf("volume %d fsync dir for %s: %v", v.Id, datFileName, err)
	}

	// Trim the remote file reference and persist the .vif (util.WriteFile fsyncs it)
	// BEFORE deleting the remote object. After this point hasRemoteFile is false, so a
	// crash before DeleteFile merely leaks the remote object while the volume reloads
	// its local .dat. The volume must NEVER be left with a .vif referencing the remote
	// object while that object is deleted.
	v.GetVolumeInfo().Files = v.GetVolumeInfo().Files[1:]
	if err := v.SaveVolumeInfo(); err != nil {
		return fmt.Errorf("volume %d failed to save remote file info: %v", v.Id, err)
	}

	// fsync the directory again so the rewritten .vif is durable.
	if err := fsyncDir(filepath.Dir(datFileName)); err != nil {
		return fmt.Errorf("volume %d fsync dir after saving volume info: %v", v.Id, err)
	}

	// Swap the data backend from the remote storage to the now-local .dat on BOTH
	// paths, so a KeepRemoteDatFile=true download still leaves the replica serving
	// from local disk (hasRemoteFile=false) rather than the shared remote object.
	if err := swapToLocalDatBackend(v, datFileName); err != nil {
		return fmt.Errorf("volume %d failed to open local dat file %s: %v", v.Id, datFileName, err)
	}

	if req.KeepRemoteDatFile {
		// Surviving replicas still reference this object; keep it intact.
		return nil
	}

	// remove remote file: only the last replica to download deletes the shared object.
	if err := backendStorage.DeleteFile(storageKey); err != nil {
		return fmt.Errorf("volume %d failed to delete remote file %s: %v", v.Id, storageKey, err)
	}

	return nil
}

// swapToLocalDatBackend closes the remote data backend and opens the downloaded
// local .dat as a DiskFile so reads are served from local disk.
func swapToLocalDatBackend(v *storage.Volume, datFileName string) error {
	dataFile, err := os.OpenFile(datFileName, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	// Swap under the volume's data lock so concurrent reads never see a closed
	// or half-swapped backend.
	v.SwapDataBackend(backend.NewDiskFile(dataFile))
	return nil
}

// fsyncDir flushes a directory entry so renamed/created files within it survive a crash.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
