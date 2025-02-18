package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"

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

	volume.move -source <source volume server host:port> -target <target volume server host:port> -volumeId <volume id>
	volume.move -source <source volume server host:port> -target <target volume server host:port> -volumeId <volume id> -disk [hdd|ssd|<tag>]

	This command move a live volume from one volume server to another volume server. Here are the steps:

	1. This command asks the target volume server to copy the source volume from source volume server, remember the last entry's timestamp.
	2. This command asks the target volume server to mount the new volume
		Now the master will mark this volume id as readonly.
	3. This command asks the target volume server to tail the source volume for updates after the timestamp, for 1 minutes to drain the requests.
	4. This command asks the source volume server to unmount the source volume
		Now the master will mark this volume id as writable.
	5. This command asks the source volume server to delete the source volume

	The option "-disk [hdd|ssd|<tag>]" can be used to change the volume disk type.

`
}

func (c *commandVolumeMove) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeMove) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volMoveCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdInt := volMoveCommand.Int("volumeId", 0, "the volume id")
	sourceNodeStr := volMoveCommand.String("source", "", "the source volume server <host>:<port>")
	targetNodeStr := volMoveCommand.String("target", "", "the target volume server <host>:<port>")
	diskTypeStr := volMoveCommand.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	ioBytePerSecond := volMoveCommand.Int64("ioBytePerSecond", 0, "limit the speed of move")
	noLock := volMoveCommand.Bool("noLock", false, "do not lock the admin shell at one's own risk")

	if err = volMoveCommand.Parse(args); err != nil {
		return nil
	}

	if *noLock {
		commandEnv.noLock = true
	} else {
		if err = commandEnv.confirmIsLocked(args); err != nil {
			return
		}
	}

	sourceVolumeServer, targetVolumeServer := pb.ServerAddress(*sourceNodeStr), pb.ServerAddress(*targetNodeStr)

	volumeId := needle.VolumeId(*volumeIdInt)

	if sourceVolumeServer == targetVolumeServer {
		return fmt.Errorf("source and target volume servers are the same!")
	}

	return LiveMoveVolume(commandEnv.option.GrpcDialOption, writer, volumeId, sourceVolumeServer, targetVolumeServer, 5*time.Second, *diskTypeStr, *ioBytePerSecond, false)
}

// LiveMoveVolume moves one volume from one source volume server to one target volume server, with idleTimeout to drain the incoming requests.
func LiveMoveVolume(grpcDialOption grpc.DialOption, writer io.Writer, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer pb.ServerAddress, idleTimeout time.Duration, diskType string, ioBytePerSecond int64, skipTailError bool) (err error) {

	log.Printf("copying volume %d from %s to %s", volumeId, sourceVolumeServer, targetVolumeServer)
	lastAppendAtNs, err := copyVolume(grpcDialOption, writer, volumeId, sourceVolumeServer, targetVolumeServer, diskType, ioBytePerSecond)
	if err != nil {
		return fmt.Errorf("copy volume %d from %s to %s: %v", volumeId, sourceVolumeServer, targetVolumeServer, err)
	}

	log.Printf("tailing volume %d from %s to %s", volumeId, sourceVolumeServer, targetVolumeServer)
	if err = tailVolume(grpcDialOption, volumeId, sourceVolumeServer, targetVolumeServer, lastAppendAtNs, idleTimeout); err != nil {
		if skipTailError {
			fmt.Fprintf(writer, "tail volume %d from %s to %s: %v\n", volumeId, sourceVolumeServer, targetVolumeServer, err)
		} else {
			return fmt.Errorf("tail volume %d from %s to %s: %v", volumeId, sourceVolumeServer, targetVolumeServer, err)
		}
	}

	log.Printf("deleting volume %d from %s", volumeId, sourceVolumeServer)
	if err = deleteVolume(grpcDialOption, volumeId, sourceVolumeServer, false); err != nil {
		return fmt.Errorf("delete volume %d from %s: %v", volumeId, sourceVolumeServer, err)
	}

	log.Printf("moved volume %d from %s to %s", volumeId, sourceVolumeServer, targetVolumeServer)
	return nil
}

func copyVolume(grpcDialOption grpc.DialOption, writer io.Writer, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer pb.ServerAddress, diskType string, ioBytePerSecond int64) (lastAppendAtNs uint64, err error) {

	// check to see if the volume is already read-only and if its not then we need
	// to mark it as read-only and then before we return we need to undo what we
	// did
	var shouldMarkWritable bool
	defer func() {
		if !shouldMarkWritable {
			return
		}

		clientErr := operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
			_, writableErr := volumeServerClient.VolumeMarkWritable(context.Background(), &volume_server_pb.VolumeMarkWritableRequest{
				VolumeId: uint32(volumeId),
			})
			return writableErr
		})
		if clientErr != nil {
			log.Printf("failed to mark volume %d as writable after copy from %s: %v", volumeId, sourceVolumeServer, clientErr)
		}
	}()

	err = operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, statusErr := volumeServerClient.VolumeStatus(context.Background(), &volume_server_pb.VolumeStatusRequest{
			VolumeId: uint32(volumeId),
		})
		if statusErr == nil && !resp.IsReadOnly {
			shouldMarkWritable = true
			_, readonlyErr := volumeServerClient.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{
				VolumeId: uint32(volumeId),
				Persist:  false,
			})
			return readonlyErr
		}
		return statusErr
	})
	if err != nil {
		return
	}

	err = operation.WithVolumeServerClient(true, targetVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		stream, replicateErr := volumeServerClient.VolumeCopy(context.Background(), &volume_server_pb.VolumeCopyRequest{
			VolumeId:        uint32(volumeId),
			SourceDataNode:  string(sourceVolumeServer),
			DiskType:        diskType,
			IoBytePerSecond: ioBytePerSecond,
		})
		if replicateErr != nil {
			return replicateErr
		}
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				} else {
					return recvErr
				}
			}
			if resp.LastAppendAtNs != 0 {
				lastAppendAtNs = resp.LastAppendAtNs
			} else {
				fmt.Fprintf(writer, "%s => %s volume %d processed %s\n", sourceVolumeServer, targetVolumeServer, volumeId, util.BytesToHumanReadable(uint64(resp.ProcessedBytes)))
			}
		}

		return nil
	})

	return
}

func tailVolume(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer pb.ServerAddress, lastAppendAtNs uint64, idleTimeout time.Duration) (err error) {

	return operation.WithVolumeServerClient(true, targetVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, replicateErr := volumeServerClient.VolumeTailReceiver(context.Background(), &volume_server_pb.VolumeTailReceiverRequest{
			VolumeId:           uint32(volumeId),
			SinceNs:            lastAppendAtNs,
			IdleTimeoutSeconds: uint32(idleTimeout.Seconds()),
			SourceVolumeServer: string(sourceVolumeServer),
		})
		return replicateErr
	})

}

func deleteVolume(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer pb.ServerAddress, onlyEmpty bool) (err error) {
	return operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeDelete(context.Background(), &volume_server_pb.VolumeDeleteRequest{
			VolumeId:  uint32(volumeId),
			OnlyEmpty: onlyEmpty,
		})
		return deleteErr
	})
}

func markVolumeWritable(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer pb.ServerAddress, writable, persist bool) (err error) {
	return operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		if writable {
			_, err = volumeServerClient.VolumeMarkWritable(context.Background(), &volume_server_pb.VolumeMarkWritableRequest{
				VolumeId: uint32(volumeId),
			})
		} else {
			_, err = volumeServerClient.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{
				VolumeId: uint32(volumeId),
				Persist:  persist,
			})
		}
		return err
	})
}

func markVolumeReplicaWritable(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, location wdclient.Location, writable, persist bool) error {
	fmt.Printf("markVolumeReadonly %d on %s ...\n", volumeId, location.Url)
	return markVolumeWritable(grpcDialOption, volumeId, location.ServerAddress(), writable, persist)
}

func markVolumeReplicasWritable(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, locations []wdclient.Location, writable, persist bool) error {
	for _, location := range locations {
		if err := markVolumeReplicaWritable(grpcDialOption, volumeId, location, writable, persist); err != nil {
			return err
		}
	}
	return nil
}
