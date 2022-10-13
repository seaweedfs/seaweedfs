package weed_server

import (
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// VolumeTierMoveDatToRemote copy dat file to a remote tier
func (vs *VolumeServer) VolumeTierMoveDatToRemote(req *volume_server_pb.VolumeTierMoveDatToRemoteRequest, stream volume_server_pb.VolumeServer_VolumeTierMoveDatToRemoteServer) error {

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
	diskFile, ok := v.DataBackend.(*backend.DiskFile)
	if !ok {
		return nil // already copied to remove. fmt.Errorf("volume %d is not on local disk", req.VolumeId)
	}

	// check valid storage backend type
	backendStorage, found := backend.BackendStorages[req.DestinationBackendName]
	if !found {
		var keys []string
		for key := range backend.BackendStorages {
			keys = append(keys, key)
		}
		return fmt.Errorf("destination %s not found, supported: %v", req.DestinationBackendName, keys)
	}

	// check whether the existing backend storage is the same as requested
	// if same, skip
	backendType, backendId := backend.BackendNameToTypeId(req.DestinationBackendName)
	for _, remoteFile := range v.GetVolumeInfo().GetFiles() {
		if remoteFile.BackendType == backendType && remoteFile.BackendId == backendId {
			return fmt.Errorf("destination %s already exists", req.DestinationBackendName)
		}
	}

	startTime := time.Now()
	fn := func(progressed int64, percentage float32) error {
		now := time.Now()
		if now.Sub(startTime) < time.Second {
			return nil
		}
		startTime = now
		return stream.Send(&volume_server_pb.VolumeTierMoveDatToRemoteResponse{
			Processed:           progressed,
			ProcessedPercentage: percentage,
		})
	}

	// copy the data file
	key, size, err := backendStorage.CopyFile(diskFile.File, fn)
	if err != nil {
		return fmt.Errorf("backend %s copy file %s: %v", req.DestinationBackendName, diskFile.Name(), err)
	}

	// save the remote file to volume tier info
	v.GetVolumeInfo().Files = append(v.GetVolumeInfo().GetFiles(), &volume_server_pb.RemoteFile{
		BackendType:  backendType,
		BackendId:    backendId,
		Key:          key,
		Offset:       0,
		FileSize:     uint64(size),
		ModifiedTime: uint64(time.Now().Unix()),
		Extension:    ".dat",
	})

	if err := v.SaveVolumeInfo(); err != nil {
		return fmt.Errorf("volume %d failed to save remote file info: %v", v.Id, err)
	}

	if err := v.LoadRemoteFile(); err != nil {
		return fmt.Errorf("volume %d failed to load remote file: %v", v.Id, err)
	}

	if !req.KeepLocalDatFile {
		os.Remove(v.FileName(".dat"))
	}

	return nil
}
