package storage

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	_ "github.com/chrislusf/seaweedfs/weed/storage/backend/s3_backend"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func (v *Volume) GetVolumeInfo() *volume_server_pb.VolumeInfo {
	return v.volumeInfo
}

func (v *Volume) maybeLoadVolumeInfo() (found bool) {

	var err error
	v.volumeInfo, v.hasRemoteFile, found, err = pb.MaybeLoadVolumeInfo(v.FileName(".vif"))

	if v.volumeInfo.Version == 0 {
		v.volumeInfo.Version = uint32(needle.CurrentVersion)
	}

	if v.hasRemoteFile {
		glog.V(0).Infof("volume %d is tiered to %s as %s and read only", v.Id,
			v.volumeInfo.Files[0].BackendName(), v.volumeInfo.Files[0].Key)
	}

	if err != nil {
		glog.Warningf("load volume %d.vif file: %v", v.Id, err)
		return
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
