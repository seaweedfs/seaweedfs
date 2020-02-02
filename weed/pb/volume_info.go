package pb

import (
	"bytes"
	"fmt"
	"io/ioutil"

	_ "github.com/chrislusf/seaweedfs/weed/storage/backend/s3_backend"
	"github.com/chrislusf/seaweedfs/weed/util"

	"github.com/golang/protobuf/jsonpb"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
)

// MaybeLoadVolumeInfo load the file data as *volume_server_pb.VolumeInfo, the returned volumeInfo will not be nil
func MaybeLoadVolumeInfo(fileName string) (*volume_server_pb.VolumeInfo, bool, error) {

	volumeInfo := &volume_server_pb.VolumeInfo{}

	glog.V(1).Infof("maybeLoadVolumeInfo checks %s", fileName)
	if exists, canRead, _, _, _ := util.CheckFile(fileName); !exists || !canRead {
		if !exists {
			return volumeInfo, false, nil
		}
		if !canRead {
			glog.Warningf("can not read %s", fileName)
			return volumeInfo, false, fmt.Errorf("can not read %s", fileName)
		}
		return volumeInfo, false, nil
	}

	glog.V(1).Infof("maybeLoadVolumeInfo reads %s", fileName)
	tierData, readErr := ioutil.ReadFile(fileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", fileName, readErr)
		return volumeInfo, false, fmt.Errorf("fail to read %s : %v", fileName, readErr)
	}

	glog.V(1).Infof("maybeLoadVolumeInfo Unmarshal volume info %v", fileName)
	if err := jsonpb.Unmarshal(bytes.NewReader(tierData), volumeInfo); err != nil {
		glog.Warningf("unmarshal error: %v", err)
		return volumeInfo, false, fmt.Errorf("unmarshal error: %v", err)
	}

	if len(volumeInfo.GetFiles()) == 0 {
		return volumeInfo, false, nil
	}

	return volumeInfo, true, nil
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

	writeErr := ioutil.WriteFile(fileName, []byte(text), 0755)
	if writeErr != nil {
		return fmt.Errorf("fail to write %s : %v", fileName, writeErr)
	}

	return nil
}
