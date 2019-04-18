package weed_server

import (
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func (vs *VolumeServer) VolumeStreamFollow(req *volume_server_pb.VolumeStreamFollowRequest, stream volume_server_pb.VolumeServer_VolumeStreamFollowServer) error {

	v := vs.store.GetVolume(storage.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	lastTimestampNs := req.SinceNs
	drainingSeconds := req.DrainingSeconds

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:
			lastProcessedTimestampNs, err := sendNeedlesSince(stream, v, lastTimestampNs)
			if err != nil {
				return fmt.Errorf("streamFollow: %v", err)
			}
			if req.DrainingSeconds == 0 {
				continue
			}
			if lastProcessedTimestampNs == lastTimestampNs {
				drainingSeconds--
				if drainingSeconds <= 0 {
					return nil
				}
				glog.V(0).Infof("volume %d drains requests with %d seconds remaining ...", v.Id, drainingSeconds)
			} else {
				drainingSeconds = req.DrainingSeconds
				glog.V(0).Infof("volume %d resets draining wait time to %d seconds", v.Id, drainingSeconds)
			}
		}
	}

}

func sendNeedlesSince(stream volume_server_pb.VolumeServer_VolumeStreamFollowServer, v *storage.Volume, lastTimestampNs uint64) (lastProcessedTimestampNs uint64, err error) {

	foundOffset, isLastOne, err := v.BinarySearchByAppendAtNs(lastTimestampNs)
	if err != nil {
		return 0, fmt.Errorf("fail to locate by appendAtNs %d: %s", lastTimestampNs, err)
	}

	if isLastOne {
		return lastTimestampNs, nil
	}

	err = storage.ScanVolumeFileNeedleFrom(v.Version(), v.DataFile(), foundOffset.ToAcutalOffset(), func(needleHeader, needleBody []byte, needleAppendAtNs uint64) error {

		sendErr := stream.Send(&volume_server_pb.VolumeStreamFollowResponse{
			NeedleHeader: needleHeader,
			NeedleBody:   needleBody,
		})
		if sendErr != nil {
			return sendErr
		}

		lastProcessedTimestampNs = needleAppendAtNs
		return nil

	})

	return

}
