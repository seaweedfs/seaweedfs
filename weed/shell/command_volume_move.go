package shell

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

func init() {
	commands = append(commands, &commandVolumeMove{})
}

type commandVolumeMove struct {
	grpcDialOption grpc.DialOption
}

func (c *commandVolumeMove) Name() string {
	return "volume.move"
}

func (c *commandVolumeMove) Help() string {
	return `move a live volume from one volume server to another volume server

	volume.move <source volume server host:port> <target volume server host:port> <volume id>

	This command move a live volume from one volume server to another volume server. Here are the steps:

	1. This command asks the target volume server to copy the source volume from source volume server, remember the last entry's timestamp.
	2. This command asks the target volume server to mount the new volume
		Now the master will mark this volume id as readonly.
	3. This command asks the target volume server to tail the source volume for updates after the timestamp, for 1 minutes to drain the requests.
	4. This command asks the source volume server to unmount the source volume
		Now the master will mark this volume id as writable.
	5. This command asks the source volume server to delete the source volume

`
}

func (c *commandVolumeMove) Do(args []string, commandEnv *commandEnv, writer io.Writer) (err error) {

	if len(args) != 3 {
		fmt.Fprintf(writer, "received args: %+v\n", args)
		return fmt.Errorf("need 3 args of <source volume server host:port> <target volume server host:port> <volume id>")
	}
	c.grpcDialOption = commandEnv.option.GrpcDialOption
	ctx := context.Background()

	sourceVolumeServer, targetVolumeServer, volumeIdString := args[0], args[1], args[2]

	volumeId, err := needle.NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("wrong volume id format %s: %v", volumeId, err)
	}

	if sourceVolumeServer == targetVolumeServer {
		return fmt.Errorf("source and target volume servers are the same!")
	}

	log.Printf("copying volume %d from %s to %s", volumeId, sourceVolumeServer, targetVolumeServer)
	lastAppendAtNs, err := c.copyVolume(ctx, volumeId, sourceVolumeServer, targetVolumeServer)
	if err != nil {
		return fmt.Errorf("copy volume %d from %s to %s: %v", volumeId, sourceVolumeServer, targetVolumeServer, err)
	}

	log.Printf("tailing volume %d from %s to %s", volumeId, sourceVolumeServer, targetVolumeServer)
	if err = c.tailVolume(ctx, volumeId, sourceVolumeServer, targetVolumeServer, lastAppendAtNs, 5*time.Second); err != nil {
		return fmt.Errorf("tail volume %d from %s to %s: %v", volumeId, sourceVolumeServer, targetVolumeServer, err)
	}

	log.Printf("deleting volume %d from %s", volumeId, sourceVolumeServer)
	if err = c.deleteVolume(ctx, volumeId, sourceVolumeServer); err != nil {
		return fmt.Errorf("delete volume %d from %s: %v", volumeId, sourceVolumeServer, err)
	}

	log.Printf("moved volume %d from %s to %s", volumeId, sourceVolumeServer, targetVolumeServer)

	return nil
}

func (c *commandVolumeMove) copyVolume(ctx context.Context, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer string) (lastAppendAtNs uint64, err error) {

	err = operation.WithVolumeServerClient(targetVolumeServer, c.grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, replicateErr := volumeServerClient.VolumeCopy(ctx, &volume_server_pb.VolumeCopyRequest{
			VolumeId:       uint32(volumeId),
			SourceDataNode: sourceVolumeServer,
		})
		if replicateErr == nil {
			lastAppendAtNs = resp.LastAppendAtNs
		}
		return replicateErr
	})

	return
}

func (c *commandVolumeMove) tailVolume(ctx context.Context, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer string, lastAppendAtNs uint64, timeout time.Duration) (err error) {

	return operation.WithVolumeServerClient(targetVolumeServer, c.grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, replicateErr := volumeServerClient.VolumeTailReceiver(ctx, &volume_server_pb.VolumeTailReceiverRequest{
			VolumeId:           uint32(volumeId),
			SinceNs:            lastAppendAtNs,
			DrainingSeconds:    uint32(timeout.Seconds()),
			SourceVolumeServer: sourceVolumeServer,
		})
		return replicateErr
	})

}

func (c *commandVolumeMove) deleteVolume(ctx context.Context, volumeId needle.VolumeId, sourceVolumeServer string) (err error) {
	return operation.WithVolumeServerClient(sourceVolumeServer, c.grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, unmountErr := volumeServerClient.VolumeDelete(ctx, &volume_server_pb.VolumeDeleteRequest{
			VolumeId: uint32(volumeId),
		})
		return unmountErr
	})
}
