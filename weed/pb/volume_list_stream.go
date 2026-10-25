package pb

import (
	"context"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReceiveVolumeList reads a streamed volume listing: the topology first, then
// its volumes in batches as they arrive. A caller that works a volume at a
// time never holds the cluster; one that needs it whole can use
// CollectVolumeList.
//
// onTopology is given a listing whose disks name themselves but list nothing,
// because the volumes come through onVolumes instead. That holds however the
// master answered: one too old for the stream is asked the old way and its
// reply cut into the same batches, so a caller cannot tell the difference and
// must not read volumes off the topology either way.
func ReceiveVolumeList(ctx context.Context, client master_pb.SeaweedClient, request *master_pb.VolumeListRequest,
	onTopology func(*master_pb.VolumeListResponse) error,
	onVolumes func(*master_pb.VolumeListStreamResponse) error) error {

	stream, err := client.VolumeListStream(ctx, request)
	if err == nil {
		started, streamErr := receiveVolumeListStream(stream, onTopology, onVolumes)
		// Only a stream that said nothing can be asked again the old way. Past
		// its first message the master plainly does have the method, and the
		// error may even be the caller's own, so starting over would hand back
		// what has already been handed over.
		if started || status.Code(streamErr) != codes.Unimplemented {
			return streamErr
		}
	} else if status.Code(err) != codes.Unimplemented {
		return err
	}

	response, err := client.VolumeList(ctx, request)
	if err != nil {
		return err
	}
	return replayVolumeList(response, onTopology, onVolumes)
}

// receiveVolumeListStream reports whether the stream said anything at all,
// which decides whether it can be started over as an unstreamed listing.
func receiveVolumeListStream(stream master_pb.Seaweed_VolumeListStreamClient,
	onTopology func(*master_pb.VolumeListResponse) error,
	onVolumes func(*master_pb.VolumeListStreamResponse) error) (started bool, err error) {

	told := false
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			if !told {
				return started, fmt.Errorf("volume list stream ended before its topology")
			}
			return started, nil
		}
		if err != nil {
			return started, err
		}
		started = true
		if batch.Header != nil {
			if told {
				return started, fmt.Errorf("volume list stream sent its topology twice")
			}
			told = true
			if onTopology != nil {
				if err := onTopology(batch.Header); err != nil {
					return started, err
				}
			}
			continue
		}
		if !told {
			return started, fmt.Errorf("volume list stream sent volumes before its topology")
		}
		if onVolumes != nil {
			if err := onVolumes(batch); err != nil {
				return started, err
			}
		}
	}
}

// replayVolumeList cuts an unstreamed reply into the batches a caller expects.
// Each disk's volumes are moved out of the topology rather than shared with it,
// so the topology handed over lists nothing, exactly as a streamed one does.
func replayVolumeList(response *master_pb.VolumeListResponse,
	onTopology func(*master_pb.VolumeListResponse) error,
	onVolumes func(*master_pb.VolumeListStreamResponse) error) error {

	type batch struct {
		key    [4]string
		volume []*master_pb.VolumeInformationMessage
		ec     []*master_pb.VolumeEcShardInformationMessage
	}
	var batches []batch

	if response.TopologyInfo != nil {
		for _, dc := range response.TopologyInfo.DataCenterInfos {
			for _, rack := range dc.RackInfos {
				for _, node := range rack.DataNodeInfos {
					for diskType, disk := range node.DiskInfos {
						if len(disk.VolumeInfos) == 0 && len(disk.EcShardInfos) == 0 {
							continue
						}
						batches = append(batches, batch{
							key:    [4]string{dc.Id, rack.Id, node.Id, diskType},
							volume: disk.VolumeInfos,
							ec:     disk.EcShardInfos,
						})
						disk.VolumeInfos, disk.EcShardInfos = nil, nil
					}
				}
			}
		}
	}

	if onTopology != nil {
		if err := onTopology(response); err != nil {
			return err
		}
	}
	if onVolumes == nil {
		return nil
	}
	for _, b := range batches {
		err := onVolumes(&master_pb.VolumeListStreamResponse{
			DataCenter:   b.key[0],
			Rack:         b.key[1],
			DataNode:     b.key[2],
			DiskType:     b.key[3],
			VolumeInfos:  b.volume,
			EcShardInfos: b.ec,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// CollectVolumeList streams a listing and puts it back together, for callers
// that need the whole topology. The master still never holds it all, which is
// the point; this only moves that cost to the caller.
func CollectVolumeList(ctx context.Context, client master_pb.SeaweedClient, request *master_pb.VolumeListRequest) (*master_pb.VolumeListResponse, error) {
	var response *master_pb.VolumeListResponse
	var skipped int
	disks := make(map[[4]string]*master_pb.DiskInfo)

	err := ReceiveVolumeList(ctx, client, request,
		func(topology *master_pb.VolumeListResponse) error {
			response = topology
			return nil
		},
		func(batch *master_pb.VolumeListStreamResponse) error {
			key := [4]string{batch.DataCenter, batch.Rack, batch.DataNode, batch.DiskType}
			disk, known := disks[key]
			if !known {
				disk = findDisk(response, key)
				disks[key] = disk
			}
			if disk == nil {
				// A disk registered after the topology went out. It is not in
				// the listing being rebuilt and has nowhere to go, so leave it
				// to the next one rather than failing this one -- an unstreamed
				// listing would not have shown it either, having read each
				// node's disks once.
				skipped++
				return nil
			}
			disk.VolumeInfos = append(disk.VolumeInfos, batch.VolumeInfos...)
			disk.EcShardInfos = append(disk.EcShardInfos, batch.EcShardInfos...)
			return nil
		})
	if err != nil {
		return nil, err
	}
	if skipped > 0 {
		glog.V(1).Infof("volume list: %d batches were for disks added after the topology was sent", skipped)
	}
	return response, nil
}

func findDisk(response *master_pb.VolumeListResponse, key [4]string) *master_pb.DiskInfo {
	if response == nil || response.TopologyInfo == nil {
		return nil
	}
	for _, dc := range response.TopologyInfo.DataCenterInfos {
		if dc.Id != key[0] {
			continue
		}
		for _, rack := range dc.RackInfos {
			if rack.Id != key[1] {
				continue
			}
			for _, node := range rack.DataNodeInfos {
				if node.Id != key[2] {
					continue
				}
				return node.DiskInfos[key[3]]
			}
		}
	}
	return nil
}
