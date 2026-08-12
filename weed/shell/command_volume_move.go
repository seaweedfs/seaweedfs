package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/operation/volume_move"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"

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

	1. This command marks the source volume as read-only, copies it to the target volume server, and records the last entry timestamp.
	2. This command asks the target volume server to mount the new volume.
	3. This command asks the target volume server to tail the source volume for updates after the timestamp, for 1 minutes to drain any in-flight requests.
	4. This command verifies the target volume matches the source, then asks the source volume server to delete the source volume.

	The option "-disk [hdd|ssd|<tag>]" can be used to change the volume disk type.
	The option "-timeout" fails the whole move if it does not finish in time.

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
	timeout := volMoveCommand.Duration("timeout", 0, "wall-clock cap on the whole move; 0 = no timeout")
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

	if volume_move.SameServer(sourceVolumeServer, targetVolumeServer) {
		return fmt.Errorf("source and target volume servers are the same!")
	}

	ctx := context.Background()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	return LiveMoveVolume(ctx, commandEnv.option.GrpcDialOption, writer, volumeId, sourceVolumeServer, targetVolumeServer, 5*time.Second, *diskTypeStr, *ioBytePerSecond)
}

// LiveMoveVolume moves one volume from one source volume server to one target volume server, with idleTimeout to drain the incoming requests.
func LiveMoveVolume(ctx context.Context, grpcDialOption grpc.DialOption, writer io.Writer, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer pb.ServerAddress, idleTimeout time.Duration, diskType string, ioBytePerSecond int64) (err error) {
	return volume_move.NewMover(grpcDialOption).LiveMoveVolume(ctx, volumeId, sourceVolumeServer, targetVolumeServer, volume_move.VolumeMoveOptions{
		DiskType:        diskType,
		IoBytePerSecond: ioBytePerSecond,
		IdleTimeout:     idleTimeout,
		Writer:          writer,
	})
}

func copyVolume(ctx context.Context, grpcDialOption grpc.DialOption, writer io.Writer, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer pb.ServerAddress, diskType string, ioBytePerSecond int64, restoreWritable bool) (lastAppendAtNs uint64, err error) {
	return volume_move.NewMover(grpcDialOption).CopyVolume(ctx, volumeId, sourceVolumeServer, targetVolumeServer, diskType, ioBytePerSecond, restoreWritable, writer)
}

func tailVolume(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer, targetVolumeServer pb.ServerAddress, lastAppendAtNs uint64, idleTimeout time.Duration) (err error) {
	return volume_move.NewMover(grpcDialOption).TailVolume(ctx, volumeId, sourceVolumeServer, targetVolumeServer, lastAppendAtNs, idleTimeout)
}

// deleteVolume removes the volume from sourceVolumeServer. When keepRemoteData
// is true, the cloud-tier object backing the volume is left intact — used on
// the source side of a move where another server is taking over the same .vif.
func deleteVolume(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer pb.ServerAddress, onlyEmpty bool, keepRemoteData bool) (err error) {
	return volume_move.NewMover(grpcDialOption).DeleteVolume(ctx, volumeId, sourceVolumeServer, onlyEmpty, keepRemoteData)
}

func markVolumeWritable(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer pb.ServerAddress, writable, persist bool) (err error) {
	return markVolumeState(ctx, grpcDialOption, volumeId, sourceVolumeServer, writable, false, persist)
}

// canDelete: readonly that still accepts deletes, so expiring data drains the volume.
func markVolumeState(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer pb.ServerAddress, writable, canDelete, persist bool) (err error) {
	return operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		if writable {
			_, err = volumeServerClient.VolumeMarkWritable(ctx, &volume_server_pb.VolumeMarkWritableRequest{
				VolumeId: uint32(volumeId),
			})
		} else {
			_, err = volumeServerClient.VolumeMarkReadonly(ctx, &volume_server_pb.VolumeMarkReadonlyRequest{
				VolumeId:  uint32(volumeId),
				Persist:   persist,
				CanDelete: canDelete,
			})
		}
		return err
	})
}

func markVolumeReplicaWritable(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, location wdclient.Location, writable, persist bool) error {
	if writable {
		fmt.Printf("markVolumeWritable %d on %s ...\n", volumeId, location.Url)
	} else {
		fmt.Printf("markVolumeReadonly %d on %s persist=%v ...\n", volumeId, location.Url, persist)
	}
	return markVolumeWritable(ctx, grpcDialOption, volumeId, location.ServerAddress(), writable, persist)
}

func markVolumeReplicasWritable(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, locations []wdclient.Location, writable, persist bool) error {
	for _, location := range locations {
		if err := markVolumeReplicaWritable(ctx, grpcDialOption, volumeId, location, writable, persist); err != nil {
			return err
		}
	}
	return nil
}

// replicateVolumeToServer copies a volume from sourceAddress to targetAddress via the VolumeCopy gRPC stream.
func replicateVolumeToServer(ctx context.Context, grpcDialOption grpc.DialOption, writer io.Writer, volumeId needle.VolumeId, sourceAddress, targetAddress pb.ServerAddress, diskType string) error {
	return volume_move.NewMover(grpcDialOption).ReplicateVolume(ctx, volumeId, sourceAddress, targetAddress, diskType, writer)
}

// configureVolumeReplication sets the replication setting on a volume at the given server.
func configureVolumeReplication(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, targetAddress pb.ServerAddress, replicationString string) error {
	return volume_move.NewMover(grpcDialOption).ConfigureVolumeReplication(ctx, volumeId, targetAddress, replicationString)
}
