package weed_server

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
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

	scanner := &VolumeFileScanner4Tailing{
		stream: stream,
	}

	err = storage.ScanVolumeFileFrom(v.Version(), v.DataBackend, foundOffset.ToActualOffset(), scanner)

	return scanner.lastProcessedTimestampNs, err

}

func (vs *VolumeServer) VolumeTailReceiver(ctx context.Context, req *volume_server_pb.VolumeTailReceiverRequest) (*volume_server_pb.VolumeTailReceiverResponse, error) {

	resp := &volume_server_pb.VolumeTailReceiverResponse{}

	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return resp, fmt.Errorf("receiver not found volume id %d", req.VolumeId)
	}

	defer glog.V(1).Infof("receive tailing volume %d finished", v.Id)

	return resp, operation.TailVolumeFromSource(pb.ServerAddress(req.SourceVolumeServer), vs.grpcDialOption, v.Id, req.SinceNs, int(req.IdleTimeoutSeconds), func(n *needle.Needle) error {
		_, err := vs.store.WriteVolumeNeedle(v.Id, n, false, false)
		return err
	})

}

// generate the volume idx
type VolumeFileScanner4Tailing struct {
	stream                   volume_server_pb.VolumeServer_VolumeTailSenderServer
	lastProcessedTimestampNs uint64
}

func (scanner *VolumeFileScanner4Tailing) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	return nil

}
func (scanner *VolumeFileScanner4Tailing) ReadNeedleBody() bool {
	return true
}

func (scanner *VolumeFileScanner4Tailing) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	isLastChunk := false

	// need to send body by chunks
	for i := 0; i < len(needleBody); i += BufferSizeLimit {
		stopOffset := i + BufferSizeLimit
		if stopOffset >= len(needleBody) {
			isLastChunk = true
			stopOffset = len(needleBody)
		}

		sendErr := scanner.stream.Send(&volume_server_pb.VolumeTailSenderResponse{
			NeedleHeader: needleHeader,
			NeedleBody:   needleBody[i:stopOffset],
			IsLastChunk:  isLastChunk,
		})
		if sendErr != nil {
			return sendErr
		}
	}

	scanner.lastProcessedTimestampNs = n.AppendAtNs
	return nil
}
