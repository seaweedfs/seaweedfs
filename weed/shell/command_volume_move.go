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
	Commands = append(Commands, &commandVolumeMove{})
}

type commandVolumeMove struct {
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

func (c *commandVolumeMove) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	if err = commandEnv.confirmIsLocked(); err != nil {
		return
	}

	if len(args) != 3 {
		fmt.Fprintf(writer, "received args: %+v\n", args)
		return fmt.Errorf("need 3 args of <source volume server host:port> <target volume server host:port> <volume id>")
	}
	sourceVolumeServer, targetVolumeServer, volumeIdString := args[0], args[1], args[2]

	volumeId, err := needle.NewVolumeId(volumeIdString)
	if err != nil {
		return fmt.Errorf("wrong volume id format %s: %v", volumeId, err)
	}

	if sourceVolumeServer == targetVolumeServer {
		return fmt.Errorf("source and target volume servers are the same!")
	}

	return LiveMoveVolume(commandEnv.option.GrpcDialOption, volumeId, sourceVolumeServer, targetVolumeServer, 5*time.Second)
}

// LiveMoveVolume moves one volume from one source volume server to one target volume server, with idleTimeout to drain the incoming requests.
func LiveMoveVolume(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer string, idleTimeout time.Duration) (err error) {

	log.Printf("copying volume %d from %s to %s", volumeId, sourceVolumeServer, targetVolumeServer)
	lastAppendAtNs, err := copyVolume(grpcDialOption, volumeId, sourceVolumeServer, targetVolumeServer)
	if err != nil {
		return fmt.Errorf("copy volume %d from %s to %s: %v", volumeId, sourceVolumeServer, targetVolumeServer, err)
	}

	log.Printf("tailing volume %d from %s to %s", volumeId, sourceVolumeServer, targetVolumeServer)
	if err = tailVolume(grpcDialOption, volumeId, sourceVolumeServer, targetVolumeServer, lastAppendAtNs, idleTimeout); err != nil {
		return fmt.Errorf("tail volume %d from %s to %s: %v", volumeId, sourceVolumeServer, targetVolumeServer, err)
	}

	log.Printf("deleting volume %d from %s", volumeId, sourceVolumeServer)
	if err = deleteVolume(grpcDialOption, volumeId, sourceVolumeServer); err != nil {
		return fmt.Errorf("delete volume %d from %s: %v", volumeId, sourceVolumeServer, err)
	}

	log.Printf("moved volume %d from %s to %s", volumeId, sourceVolumeServer, targetVolumeServer)
	return nil
}

func copyVolume(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer string) (lastAppendAtNs uint64, err error) {

	err = operation.WithVolumeServerClient(targetVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, replicateErr := volumeServerClient.VolumeCopy(context.Background(), &volume_server_pb.VolumeCopyRequest{
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

func tailVolume(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer string, lastAppendAtNs uint64, idleTimeout time.Duration) (err error) {

	return operation.WithVolumeServerClient(targetVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, replicateErr := volumeServerClient.VolumeTailReceiver(context.Background(), &volume_server_pb.VolumeTailReceiverRequest{
			VolumeId:           uint32(volumeId),
			SinceNs:            lastAppendAtNs,
			IdleTimeoutSeconds: uint32(idleTimeout.Seconds()),
			SourceVolumeServer: sourceVolumeServer,
		})
		return replicateErr
	})

}

func deleteVolume(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer string) (err error) {
	return operation.WithVolumeServerClient(sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
			VolumeId: uint32(volumeId),
		})
		return deleteErr
	})
}
