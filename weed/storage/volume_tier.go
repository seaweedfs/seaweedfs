package storage

import (
	"fmt"
	"time"

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

func (v *Volume) HasRemoteFile() bool {
	return v.hasRemoteFile.Load()
}

func (v *Volume) LoadRemoteFile() error {
	tierFile := v.volumeInfo.GetFiles()[0]
	backendStorage, found := backend.BackendStorages[tierFile.BackendName()]
	if !found {
		return fmt.Errorf("backend storage %s not found", tierFile.BackendName())
	}

	// Swap under dataFileAccessLock (via SwapDataBackend) so the heartbeat's
	// concurrent DataBackend read never races this reassignment, and mark the
	// volume tiered in the same locked step so a later heartbeat does not treat
	// the just-removed local .dat as a phantom volume and stop reporting it to
	// the master. On disk-scan load this is already true; here it flips a volume
	// that was tier-uploaded in-process without a reload.
	v.SwapDataBackend(backendStorage.NewStorageFile(tierFile.Key, v.volumeInfo), true)
	return nil
}

func (v *Volume) SaveVolumeInfo() error {

	tierFileName := v.FileName(".vif")
	if v.Ttl != nil {
		ttlSeconds := v.Ttl.ToSeconds()
		if ttlSeconds > 0 {
			v.volumeInfo.ExpireAtSec = uint64(time.Now().Unix()) + ttlSeconds //calculated destroy time from the ec volume was created
		}
	}

	return volume_info.SaveVolumeInfo(tierFileName, v.volumeInfo)

}
