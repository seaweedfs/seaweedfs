package weed_server

import (
	"context"
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
)

func (vs *VolumeServer) VolumeTailSender(req *volume_server_pb.VolumeTailSenderRequest, stream volume_server_pb.VolumeServer_VolumeTailSenderServer) error {

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return fmt.Errorf("not found volume id %d", req.VolumeId)
	}

	defer glog.V(1).Infof("tailing volume %d finished", v.Id)

	lastTimestampNs := req.SinceNs
	drainingSeconds := req.IdleTimeoutSeconds

	for {
		lastProcessedTimestampNs, err := sendNeedlesSince(stream, v, lastTimestampNs)
		if err != nil {
			glog.Infof("sendNeedlesSince: %v", err)
			return fmt.Errorf("streamFollow: %v", err)
		}
		time.Sleep(2 * time.Second)

		if req.IdleTimeoutSeconds == 0 {
			lastTimestampNs = lastProcessedTimestampNs
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
			drainingSeconds = req.IdleTimeoutSeconds
			glog.V(1).Infof("tailing volume %d resets draining wait time to %d seconds", v.Id, drainingSeconds)
		}

	}

}

func sendNeedlesSince(stream volume_server_pb.VolumeServer_VolumeTailSenderServer, v *storage.Volume, lastTimestampNs uint64) (lastProcessedTimestampNs uint64, err error) {

	foundOffset, isLastOne, err := v.BinarySearchByAppendAtNs(lastTimestampNs)
	if err != nil {
		return 0, fmt.Errorf("fail to locate by appendAtNs %d: %s", lastTimestampNs, err)
	}

	// log.Printf("reading ts %d offset %d isLast %v", lastTimestampNs, foundOffset, isLastOne)

	if isLastOne {
		// need to heart beat to the client to ensure the connection health
		sendErr := stream.Send(&volume_server_pb.VolumeTailSenderResponse{IsLastChunk: true})
		return lastTimestampNs, sendErr
	}

	err = storage.ScanVolumeFileNeedleFrom(v.Version(), v.DataFile(), foundOffset.ToAcutalOffset(), func(needleHeader, needleBody []byte, needleAppendAtNs uint64) error {

		blockSizeLimit := 1024 * 1024 * 2
		isLastChunk := false

		// need to send body by chunks
		for i := 0; i < len(needleBody); i += blockSizeLimit {
			stopOffset := i + blockSizeLimit
			if stopOffset >= len(needleBody) {
				isLastChunk = true
				stopOffset = len(needleBody)
			}

			sendErr := stream.Send(&volume_server_pb.VolumeTailSenderResponse{
				NeedleHeader: needleHeader,
				NeedleBody:   needleBody[i:stopOffset],
				IsLastChunk:  isLastChunk,
			})
			if sendErr != nil {
				return sendErr
			}
		}

		lastProcessedTimestampNs = needleAppendAtNs
		return nil

	})

	return

}

func (vs *VolumeServer) VolumeTailReceiver(ctx context.Context, req *volume_server_pb.VolumeTailReceiverRequest) (*volume_server_pb.VolumeTailReceiverResponse, error) {

	resp := &volume_server_pb.VolumeTailReceiverResponse{}

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return resp, fmt.Errorf("receiver not found volume id %d", req.VolumeId)
	}

	defer glog.V(1).Infof("receive tailing volume %d finished", v.Id)

	return resp, operation.TailVolumeFromSource(req.SourceVolumeServer, vs.grpcDialOption, v.Id, req.SinceNs, int(req.IdleTimeoutSeconds), func(n *needle.Needle) error {
		_, err := vs.store.Write(v.Id, n)
		return err
	})

}
