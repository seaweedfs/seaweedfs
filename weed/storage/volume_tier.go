package storage

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	_ "github.com/chrislusf/seaweedfs/weed/storage/backend/s3_backend"
)

func (v *Volume) GetVolumeInfo() *volume_server_pb.VolumeInfo {
	return v.volumeInfo
}

func (v *Volume) maybeLoadVolumeInfo() (found bool) {

	v.volumeInfo, v.hasRemoteFile, _ = pb.MaybeLoadVolumeInfo(v.FileName(".vif"))

	if v.hasRemoteFile {
		glog.V(0).Infof("volume %d is tiered to %s as %s and read only", v.Id,
			v.volumeInfo.Files[0].BackendName(), v.volumeInfo.Files[0].Key)
	}

	return

}

func (v *Volume) HasRemoteFile() bool {
	return v.hasRemoteFile
}

func (v *Volume) LoadRemoteFile() error {
	tierFile := v.volumeInfo.GetFiles()[0]
	backendStorage := backend.BackendStorages[tierFile.BackendName()]

	if v.DataBackend != nil {
		v.DataBackend.Close()
	}

	v.DataBackend = backendStorage.NewStorageFile(tierFile.Key, v.volumeInfo)
	return nil
}

func (v *Volume) SaveVolumeInfo() error {

	tierFileName := v.FileName(".vif")

	return pb.SaveVolumeInfo(tierFileName, v.volumeInfo)

}
