package weed_server

import (
	"fmt"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// clampTierConcurrency bounds a per-request transfer concurrency so a direct
// gRPC caller cannot spawn an unbounded number of network workers.
const maxTierConcurrency = 1024

func clampTierConcurrency(n int) int {
	if n < 0 {
		return 0
	}
	if n > maxTierConcurrency {
		return maxTierConcurrency
	}
	return n
}

// VolumeTierMoveDatToRemote copy dat file to a remote tier
func (vs *VolumeServer) VolumeTierMoveDatToRemote(req *volume_server_pb.VolumeTierMoveDatToRemoteRequest, stream volume_server_pb.VolumeServer_VolumeTierMoveDatToRemoteServer) error {
	if err := vs.checkGrpcAdminAuth(stream.Context()); err != nil {
		return err
	}
	if err := vs.CheckMaintenanceMode(); err != nil {
		return err
	}

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
	_, modTime, err := diskFile.GetStat()
	if err != nil {
		return fmt.Errorf("stat data file %s: %v", diskFile.Name(), err)
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
	key, size, err := backendStorage.CopyFile(diskFile.File, fn, clampTierConcurrency(int(req.Concurrency)))
	if err != nil {
		return fmt.Errorf("backend %s copy file %s: %v", req.DestinationBackendName, diskFile.Name(), err)
	}

	// record the remote file and switch to it, unless a compaction commit
	// replaced the .dat while it was being copied
	if err := v.AttachRemoteDatFile(diskFile, &volume_server_pb.RemoteFile{
		BackendType:  backendType,
		BackendId:    backendId,
		Key:          key,
		Offset:       0,
		FileSize:     uint64(size),
		ModifiedTime: uint64(modTime.Unix()),
		Extension:    ".dat",
	}); err != nil {
		// Remove the copy unless the .vif already names it.
		recorded := false
		for _, f := range v.GetVolumeInfo().GetFiles() {
			if f.Key == key && f.BackendType == backendType && f.BackendId == backendId {
				recorded = true
			}
		}
		if !recorded {
			if delErr := backendStorage.DeleteFile(key); delErr != nil {
				glog.Warningf("volume %d: remove unused remote copy %s: %v", v.Id, key, delErr)
			}
		}
		return fmt.Errorf("volume %d failed to attach remote file: %v", v.Id, err)
	}
	// Deletes must reach the index now, not only after the next restart.
	v.MarkTieredDeletable()

	if !req.KeepLocalDatFile {
		os.Remove(v.FileName(".dat"))
	}

	return nil
}
