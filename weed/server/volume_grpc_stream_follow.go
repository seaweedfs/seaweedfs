package weed_server

import (
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func (vs *VolumeServer) VolumeTail(req *volume_server_pb.VolumeTailRequest, stream volume_server_pb.VolumeServer_VolumeTailServer) error {

	v := vs.store.GetVolume(storage.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	defer glog.V(1).Infof("tailing volume %d finished", v.Id)

	lastTimestampNs := req.SinceNs
	drainingSeconds := req.DrainingSeconds

	for {
		lastProcessedTimestampNs, err := sendNeedlesSince(stream, v, lastTimestampNs)
		if err != nil {
			glog.Infof("sendNeedlesSince: %v", err)
			return fmt.Errorf("streamFollow: %v", err)
		}
		time.Sleep(2 * time.Second)

		if req.DrainingSeconds == 0 {
			continue
		}
		if lastProcessedTimestampNs == lastTimestampNs {
			drainingSeconds--
			if drainingSeconds <= 0 {
				return nil
			}
			glog.V(1).Infof("tailing volume %d drains requests with %d seconds remaining", v.Id, drainingSeconds)
		} else {
			lastTimestampNs = lastProcessedTimestampNs
			drainingSeconds = req.DrainingSeconds
			glog.V(1).Infof("tailing volume %d resets draining wait time to %d seconds", v.Id, drainingSeconds)
		}

	}

}

func sendNeedlesSince(stream volume_server_pb.VolumeServer_VolumeTailServer, v *storage.Volume, lastTimestampNs uint64) (lastProcessedTimestampNs uint64, err error) {

	foundOffset, isLastOne, err := v.BinarySearchByAppendAtNs(lastTimestampNs)
	if err != nil {
		return 0, fmt.Errorf("fail to locate by appendAtNs %d: %s", lastTimestampNs, err)
	}

	// log.Printf("reading ts %d offset %d isLast %v", lastTimestampNs, foundOffset, isLastOne)

	if isLastOne {
		// need to heart beat to the client to ensure the connection health
		sendErr := stream.Send(&volume_server_pb.VolumeTailResponse{})
		return lastTimestampNs, sendErr
	}

	err = storage.ScanVolumeFileNeedleFrom(v.Version(), v.DataFile(), foundOffset.ToAcutalOffset(), func(needleHeader, needleBody []byte, needleAppendAtNs uint64) error {

		sendErr := stream.Send(&volume_server_pb.VolumeTailResponse{
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
