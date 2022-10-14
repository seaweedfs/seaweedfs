package weed_server

import (
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
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
	// copy the data file
	_, err := backendStorage.DownloadFile(v.FileName(".dat"), storageKey, fn)
	if err != nil {
		return fmt.Errorf("backend %s copy file %s: %v", storageName, v.FileName(".dat"), err)
	}

	if req.KeepRemoteDatFile {
		return nil
	}

	// remove remote file
	if err := backendStorage.DeleteFile(storageKey); err != nil {
		return fmt.Errorf("volume %d failed to delete remote file %s: %v", v.Id, storageKey, err)
	}

	// forget remote file
	v.GetVolumeInfo().Files = v.GetVolumeInfo().Files[1:]
	if err := v.SaveVolumeInfo(); err != nil {
		return fmt.Errorf("volume %d failed to save remote file info: %v", v.Id, err)
	}

	v.DataBackend.Close()
	v.DataBackend = nil

	return nil
}
