package weed_server

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) VolumeIncrementalCopy(req *volume_server_pb.VolumeIncrementalCopyRequest, stream volume_server_pb.VolumeServer_VolumeIncrementalCopyServer) error {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	stopOffset, _, _ := v.FileStat()
	foundOffset, isLastOne, err := v.BinarySearchByAppendAtNs(req.SinceNs)
	if err != nil {
		return fmt.Errorf("fail to locate by appendAtNs %d: %s", req.SinceNs, err)
	}

	if isLastOne {
		return nil
	}

	startOffset := foundOffset.ToActualOffset()

	buf := make([]byte, 1024*1024*2)
	return sendFileContent(v.DataBackend, buf, startOffset, int64(stopOffset), stream)

}

func (vs *VolumeServer) VolumeSyncStatus(ctx context.Context, req *volume_server_pb.VolumeSyncStatusRequest) (*volume_server_pb.VolumeSyncStatusResponse, error) {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	resp := v.GetVolumeSyncStatus()

	return resp, nil

}

func sendFileContent(datBackend backend.BackendStorageFile, buf []byte, startOffset, stopOffset int64, stream volume_server_pb.VolumeServer_VolumeIncrementalCopyServer) error {
	var blockSizeLimit = int64(len(buf))
	for i := int64(0); i < stopOffset-startOffset; i += blockSizeLimit {
		n, readErr := datBackend.ReadAt(buf, startOffset+i)
		if readErr == nil || readErr == io.EOF {
			resp := &volume_server_pb.VolumeIncrementalCopyResponse{}
			resp.FileContent = buf[:int64(n)]
			sendErr := stream.Send(resp)
			if sendErr != nil {
				return sendErr
			}
		} else {
			return readErr
		}
	}
	return nil
}
