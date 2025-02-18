package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeTierDownload{})
}

type commandVolumeTierDownload struct {
}

func (c *commandVolumeTierDownload) Name() string {
	return "volume.tier.download"
}

func (c *commandVolumeTierDownload) Help() string {
	return `download the dat file of a volume from a remote tier

	volume.tier.download [-collection=""]
	volume.tier.download [-collection=""] -volumeId=<volume_id>

	e.g.:
	volume.tier.download -volumeId=7

	This command will download the dat file of a volume from a remote tier to a volume server in local cluster.

`
}

func (c *commandVolumeTierDownload) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeTierDownload) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	tierCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := tierCommand.Int("volumeId", 0, "the volume id")
	collection := tierCommand.String("collection", "", "the collection name")
	if err = tierCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	vid := needle.VolumeId(*volumeId)

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	// volumeId is provided
	if vid != 0 {
		return doVolumeTierDownload(commandEnv, writer, *collection, vid)
	}

	// apply to all volumes in the collection
	// reusing collectVolumeIdsForEcEncode for now
	volumeIds := collectRemoteVolumes(topologyInfo, *collection)
	if err != nil {
		return err
	}
	fmt.Printf("tier download volumes: %v\n", volumeIds)
	for _, vid := range volumeIds {
		if err = doVolumeTierDownload(commandEnv, writer, *collection, vid); err != nil {
			return err
		}
	}

	return nil
}

func collectRemoteVolumes(topoInfo *master_pb.TopologyInfo, selectedCollection string) (vids []needle.VolumeId) {

	vidMap := make(map[uint32]bool)
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if v.Collection == selectedCollection && v.RemoteStorageKey != "" && v.RemoteStorageName != "" {
					vidMap[v.Id] = true
				}
			}
		}
	})

	for vid := range vidMap {
		vids = append(vids, needle.VolumeId(vid))
	}

	return
}

func doVolumeTierDownload(commandEnv *CommandEnv, writer io.Writer, collection string, vid needle.VolumeId) (err error) {
	// find volume location
	locations, found := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
	if !found {
		return fmt.Errorf("volume %d not found", vid)
	}

	// TODO parallelize this
	for _, loc := range locations {
		// copy the .dat file from remote tier to local
		err = downloadDatFromRemoteTier(commandEnv.option.GrpcDialOption, writer, needle.VolumeId(vid), collection, loc.ServerAddress())
		if err != nil {
			return fmt.Errorf("download dat file for volume %d to %s: %v", vid, loc.Url, err)
		}
	}

	return nil
}

func downloadDatFromRemoteTier(grpcDialOption grpc.DialOption, writer io.Writer, volumeId needle.VolumeId, collection string, targetVolumeServer pb.ServerAddress) error {

	err := operation.WithVolumeServerClient(true, targetVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		stream, downloadErr := volumeServerClient.VolumeTierMoveDatFromRemote(context.Background(), &volume_server_pb.VolumeTierMoveDatFromRemoteRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
		})

		var lastProcessed int64
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				} else {
					return recvErr
				}
			}

			processingSpeed := float64(resp.Processed-lastProcessed) / 1024.0 / 1024.0

			fmt.Fprintf(writer, "downloaded %.2f%%, %d bytes, %.2fMB/s\n", resp.ProcessedPercentage, resp.Processed, processingSpeed)

			lastProcessed = resp.Processed
		}
		if downloadErr != nil {
			return downloadErr
		}

		_, unmountErr := volumeServerClient.VolumeUnmount(context.Background(), &volume_server_pb.VolumeUnmountRequest{
			VolumeId: uint32(volumeId),
		})
		if unmountErr != nil {
			return unmountErr
		}

		_, mountErr := volumeServerClient.VolumeMount(context.Background(), &volume_server_pb.VolumeMountRequest{
			VolumeId: uint32(volumeId),
		})
		if mountErr != nil {
			return mountErr
		}

		return nil
	})

	return err

}
