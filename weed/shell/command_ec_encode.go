package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

func init() {
	commands = append(commands, &commandEcEncode{})
}

type commandEcEncode struct {
}

func (c *commandEcEncode) Name() string {
	return "ec.encode"
}

func (c *commandEcEncode) Help() string {
	return `apply erasure coding to a volume

	This command will:
	1. freeze one volume
	2. apply erasure coding to the volume
	3. move the encoded shards to multiple volume servers

	The erasure coding is 10.4. So ideally you have more than 14 volume servers, and you can afford
	to lose 4 volume servers.

	If the number of volumes are not high, the worst case is that you only have 4 volume servers,
	and the shards are spread as 4,4,3,3, respectively. You can afford to lose one volume server.

	If you only have less than 4 volume servers, with erasure coding, at least you can afford to
	have 4 corrupted shard files.

`
}

func (c *commandEcEncode) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	encodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := encodeCommand.Int("vid", 0, "the volume id")
	if err = encodeCommand.Parse(args); err != nil {
		return nil
	}

	ctx := context.Background()

	locations := commandEnv.masterClient.GetLocations(uint32(*volumeId))

	if len(locations) == 0 {
		return fmt.Errorf("volume %d not found", *volumeId)
	}

	err = generateEcSlices(ctx, commandEnv.option.GrpcDialOption, needle.VolumeId(*volumeId), locations[0].Url)

	return err
}

func generateEcSlices(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer string) error {

	err := operation.WithVolumeServerClient(sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, genErr := volumeServerClient.VolumeEcGenerateSlices(ctx, &volume_server_pb.VolumeEcGenerateSlicesRequest{
			VolumeId: uint32(volumeId),
		})
		return genErr
	})

	return err

}
