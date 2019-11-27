package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandVolumeTier{})
}

type commandVolumeTier struct {
}

func (c *commandVolumeTier) Name() string {
	return "volume.tier"
}

func (c *commandVolumeTier) Help() string {
	return `copy the dat file of a volume to a remote tier

	ec.encode [-collection=""] [-fullPercent=95] [-quietFor=1h]
	ec.encode [-collection=""] [-volumeId=<volume_id>]

	This command will:
	1. freeze one volume
	2. copy the dat file of a volume to a remote tier

`
}

func (c *commandVolumeTier) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	tierCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := tierCommand.Int("volumeId", 0, "the volume id")
	collection := tierCommand.String("collection", "", "the collection name")
	fullPercentage := tierCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := tierCommand.Duration("quietFor", 24*time.Hour, "select volumes without no writes for this period")
	dest := tierCommand.String("destination", "", "the target tier name")
	if err = tierCommand.Parse(args); err != nil {
		return nil
	}

	ctx := context.Background()
	vid := needle.VolumeId(*volumeId)

	// volumeId is provided
	if vid != 0 {
		return doVolumeTier(ctx, commandEnv, *collection, vid, *dest)
	}

	// apply to all volumes in the collection
	// reusing collectVolumeIdsForEcEncode for now
	volumeIds, err := collectVolumeIdsForEcEncode(ctx, commandEnv, *collection, *fullPercentage, *quietPeriod)
	if err != nil {
		return err
	}
	fmt.Printf("tiering volumes: %v\n", volumeIds)
	for _, vid := range volumeIds {
		if err = doVolumeTier(ctx, commandEnv, *collection, vid, *dest); err != nil {
			return err
		}
	}

	return nil
}

func doVolumeTier(ctx context.Context, commandEnv *CommandEnv, collection string, vid needle.VolumeId, dest string) (err error) {
	// find volume location
	locations, found := commandEnv.MasterClient.GetLocations(uint32(vid))
	if !found {
		return fmt.Errorf("volume %d not found", vid)
	}

	// mark the volume as readonly
	/*
	err = markVolumeReadonly(ctx, commandEnv.option.GrpcDialOption, needle.VolumeId(vid), locations)
	if err != nil {
		return fmt.Errorf("mark volume %d as readonly on %s: %v", vid, locations[0].Url, err)
	}
	 */

	// copy the .dat file to remote tier
	err = copyDatToRemoteTier(ctx, commandEnv.option.GrpcDialOption, needle.VolumeId(vid), collection, locations[0].Url, dest)
	if err != nil {
		return fmt.Errorf("copy dat file for volume %d on %s to %s: %v", vid, locations[0].Url, dest, err)
	}

	return nil
}

func copyDatToRemoteTier(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, collection string, sourceVolumeServer string, dest string) error {

	err := operation.WithVolumeServerClient(sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, copyErr := volumeServerClient.VolumeTierCopyDatToRemote(ctx, &volume_server_pb.VolumeTierCopyDatToRemoteRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
		})
		return copyErr
	})

	return err

}
