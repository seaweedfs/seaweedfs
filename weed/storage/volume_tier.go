package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"

	_ "github.com/chrislusf/seaweedfs/weed/storage/backend/s3_backend"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/golang/protobuf/jsonpb"
)

func (v *Volume) GetVolumeInfo() *volume_server_pb.VolumeInfo {
	return v.volumeInfo
}

func (v *Volume) maybeLoadVolumeInfo() bool {

	v.volumeInfo = &volume_server_pb.VolumeInfo{}

	tierFileName := v.FileName() + ".vif"

	if exists, canRead, _, _, _ := checkFile(tierFileName); !exists || !canRead {
		if !exists {
			return false
		}
		if !canRead {
			glog.Warningf("can not read %s", tierFileName)
		}
		return false
	}

	glog.V(0).Infof("maybeLoadVolumeInfo loading volume %d check file", v.Id)

	tierData, readErr := ioutil.ReadFile(tierFileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", tierFileName, readErr)
		return false
	}

	glog.V(0).Infof("maybeLoadVolumeInfo loading volume %d ReadFile", v.Id)

	if err := jsonpb.Unmarshal(bytes.NewReader(tierData), v.volumeInfo); err != nil {
		glog.Warningf("unmarshal error: %v", err)
		return false
	}

	glog.V(0).Infof("maybeLoadVolumeInfo loading volume %d Unmarshal tierInfo %v", v.Id, v.volumeInfo)

	if len(v.volumeInfo.GetFiles()) == 0 {
		return false
	}

	glog.V(0).Infof("volume %d is tiered to %s as %s and read only", v.Id,
		v.volumeInfo.Files[0].BackendName(), v.volumeInfo.Files[0].Key)

	v.noWriteCanDelete = true
	v.noWriteOrDelete = false

	glog.V(0).Infof("loading volume %d from remote %v", v.Id, v.volumeInfo.Files)
	v.LoadRemoteFile()

	return true
}

func (v *Volume) HasRemoteFile() bool {
	if v.DataBackend == nil {
		return false
	}
	_, ok := v.DataBackend.(*backend.DiskFile)
	return !ok
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

	tierFileName := v.FileName() + ".vif"

	if exists, _, canWrite, _, _ := checkFile(tierFileName); exists && !canWrite {
		return fmt.Errorf("%s not writable", tierFileName)
	}

	m := jsonpb.Marshaler{
		EmitDefaults: true,
		Indent:       "  ",
	}

	text, marshalErr := m.MarshalToString(v.GetVolumeInfo())
	if marshalErr != nil {
		return fmt.Errorf("marshal volume %d tier info: %v", v.Id, marshalErr)
	}

	writeErr := ioutil.WriteFile(tierFileName, []byte(text), 0755)
	if writeErr != nil {
		return fmt.Errorf("fail to write %s : %v", tierFileName, writeErr)
	}

	return nil
}
