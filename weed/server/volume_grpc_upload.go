package weed_server

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"os"
)

// UploadFile client pulls the volume related file from the source server.
// if req.CompactionRevision != math.MaxUint32, it ensures the compact revision is as expected
// The copying still stop at req.StopOffset, but you can set it to math.MaxUint64 in order to read all data.
func (vs *VolumeServer) UploadFile(stream volume_server_pb.VolumeServer_UploadFileServer) error {

	location := vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
		//(location.FindEcVolume) This method is error, will cause location is nil, redundant judgment
		// _, found := location.FindEcVolume(needle.VolumeId(req.VolumeId))
		// return found
		return true
	})

	if location == nil {
		return fmt.Errorf("no space left")
	}

	var destFiles = make(map[string]*os.File)
	defer func() {
		for _, file := range destFiles {
			err := file.Close()
			if err != nil {
				return
			}
		}
	}()
	//flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			glog.V(0).Infof("stream close to req:%v", req)
			stream.SendAndClose(&volume_server_pb.UploadFileResponse{Message: "success"})
			return nil
		}
		if req == nil {
			fmt.Println("UploadFile request is nil")
			break
		}

		baseFileName := util.Join(location.Directory, erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))+req.Ext)

		//fmt.Printf("writing file:%s \n", baseFileName)
		var fileErr error
		if _, b := destFiles[baseFileName]; !b {
			glog.V(0).Infof("writing to %s", baseFileName)
			needAppend := req.Ext == ".ecj"
			if needAppend {
				flags = os.O_WRONLY | os.O_CREATE
			}
			destFiles[baseFileName], fileErr = os.OpenFile(baseFileName, flags, 0644)
			if fileErr != nil {
				fmt.Printf("writing file error:%s, %v \n", baseFileName, fileErr)
				return fileErr
			}
		}
		destFiles[baseFileName].Write(req.FileContent)
	}
	return nil
}
