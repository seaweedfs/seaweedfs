package volume_info

import (
	"bytes"
	"fmt"
	"os"

	"github.com/golang/protobuf/jsonpb"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	_ "github.com/chrislusf/seaweedfs/weed/storage/backend/s3_backend"
	"github.com/chrislusf/seaweedfs/weed/util"
)

// MaybeLoadVolumeInfo load the file data as *volume_server_pb.VolumeInfo, the returned volumeInfo will not be nil
func MaybeLoadVolumeInfo(fileName string) (volumeInfo *volume_server_pb.VolumeInfo, hasRemoteFile bool, hasVolumeInfoFile bool, err error) {

	volumeInfo = &volume_server_pb.VolumeInfo{}

	glog.V(1).Infof("maybeLoadVolumeInfo checks %s", fileName)
	if exists, canRead, _, _, _ := util.CheckFile(fileName); !exists || !canRead {
		if !exists {
			return
		}
		hasVolumeInfoFile = true
		if !canRead {
			glog.Warningf("can not read %s", fileName)
			err = fmt.Errorf("can not read %s", fileName)
			return
		}
		return
	}

	hasVolumeInfoFile = true

	glog.V(1).Infof("maybeLoadVolumeInfo reads %s", fileName)
	tierData, readErr := os.ReadFile(fileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", fileName, readErr)
		err = fmt.Errorf("fail to read %s : %v", fileName, readErr)
		return

	}

	glog.V(1).Infof("maybeLoadVolumeInfo Unmarshal volume info %v", fileName)
	if err = jsonpb.Unmarshal(bytes.NewReader(tierData), volumeInfo); err != nil {
		glog.Warningf("unmarshal error: %v", err)
		err = fmt.Errorf("unmarshal error: %v", err)
		return
	}

	if len(volumeInfo.GetFiles()) == 0 {
		return
	}

	hasRemoteFile = true

	return
}

func SaveVolumeInfo(fileName string, volumeInfo *volume_server_pb.VolumeInfo) error {

	if exists, _, canWrite, _, _ := util.CheckFile(fileName); exists && !canWrite {
		return fmt.Errorf("%s not writable", fileName)
	}

	m := jsonpb.Marshaler{
		EmitDefaults: true,
		Indent:       "  ",
	}

	text, marshalErr := m.MarshalToString(volumeInfo)
	if marshalErr != nil {
		return fmt.Errorf("marshal to %s: %v", fileName, marshalErr)
	}

	writeErr := util.WriteFile(fileName, []byte(text), 0755)
	if writeErr != nil {
		return fmt.Errorf("fail to write %s : %v", fileName, writeErr)
	}

	return nil
}
