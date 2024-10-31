package volume_info

import (
	"fmt"
	jsonpb "google.golang.org/protobuf/encoding/protojson"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/rclone_backend"
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/s3_backend"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
	fileData, readErr := os.ReadFile(fileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", fileName, readErr)
		err = fmt.Errorf("fail to read %s : %v", fileName, readErr)
		return

	}

	glog.V(1).Infof("maybeLoadVolumeInfo Unmarshal volume info %v", fileName)
	if err = jsonpb.Unmarshal(fileData, volumeInfo); err != nil {
		if oldVersionErr := tryOldVersionVolumeInfo(fileData, volumeInfo); oldVersionErr != nil {
			glog.Warningf("unmarshal error: %v oldFormat: %v", err, oldVersionErr)
			err = fmt.Errorf("unmarshal error: %v oldFormat: %v", err, oldVersionErr)
			return
		} else {
			err = nil
		}
	}

	if len(volumeInfo.GetFiles()) == 0 {
		return
	}

	hasRemoteFile = true

	return
}

func SaveVolumeInfo(fileName string, volumeInfo *volume_server_pb.VolumeInfo) error {

	if exists, _, canWrite, _, _ := util.CheckFile(fileName); exists && !canWrite {
		return fmt.Errorf("failed to check %s not writable", fileName)
	}

	m := jsonpb.MarshalOptions{
		AllowPartial:    true,
		EmitUnpopulated: true,
		Indent:          "  ",
	}

	text, marshalErr := m.Marshal(volumeInfo)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal %s: %v", fileName, marshalErr)
	}

	if err := util.WriteFile(fileName, text, 0644); err != nil {
		return fmt.Errorf("failed to write %s: %v", fileName, err)
	}

	return nil
}

func tryOldVersionVolumeInfo(data []byte, volumeInfo *volume_server_pb.VolumeInfo) error {
	oldVersionVolumeInfo := &volume_server_pb.OldVersionVolumeInfo{}
	if err := jsonpb.Unmarshal(data, oldVersionVolumeInfo); err != nil {
		return fmt.Errorf("failed to unmarshal old version volume info: %v", err)
	}
	volumeInfo.Files = oldVersionVolumeInfo.Files
	volumeInfo.Version = oldVersionVolumeInfo.Version
	volumeInfo.Replication = oldVersionVolumeInfo.Replication
	volumeInfo.BytesOffset = oldVersionVolumeInfo.BytesOffset
	volumeInfo.DatFileSize = oldVersionVolumeInfo.DatFileSize
	volumeInfo.ExpireAtSec = oldVersionVolumeInfo.DestroyTime
	volumeInfo.ReadOnly = oldVersionVolumeInfo.ReadOnly

	return nil
}
