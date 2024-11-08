package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeTierUpload{})
}

type commandVolumeTierUpload struct {
}

func (c *commandVolumeTierUpload) Name() string {
	return "volume.tier.upload"
}

func (c *commandVolumeTierUpload) Help() string {
	return `upload the dat file of a volume to a remote tier

	volume.tier.upload [-collection=""] [-fullPercent=95] [-quietFor=1h]
	volume.tier.upload [-collection=""] -volumeId=<volume_id> -dest=<storage_backend> [-keepLocalDatFile]

	e.g.:
	volume.tier.upload -volumeId=7 -dest=s3
	volume.tier.upload -volumeId=7 -dest=s3.default

	The <storage_backend> is defined in master.toml.
	For example, "s3.default" in [storage.backend.s3.default]

	This command will move the dat file of a volume to a remote tier.

	SeaweedFS enables scalable and fast local access to lots of files, 
	and the cloud storage is slower by cost efficient. How to combine them together?

	Usually the data follows 80/20 rule: only 20% of data is frequently accessed.
	We can offload the old volumes to the cloud.

	With this, SeaweedFS can be both fast and scalable, and infinite storage space.
	Just add more local SeaweedFS volume servers to increase the throughput.

	The index file is still local, and the same O(1) disk read is applied to the remote file.

`
}

func (c *commandVolumeTierUpload) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeTierUpload) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	tierCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := tierCommand.Int("volumeId", 0, "the volume id")
	collection := tierCommand.String("collection", "", "the collection name")
	fullPercentage := tierCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := tierCommand.Duration("quietFor", 24*time.Hour, "select volumes without no writes for this period")
	dest := tierCommand.String("dest", "", "the target tier name")
	keepLocalDatFile := tierCommand.Bool("keepLocalDatFile", false, "whether keep local dat file")
	if err = tierCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	vid := needle.VolumeId(*volumeId)

	// volumeId is provided
	if vid != 0 {
		return doVolumeTierUpload(commandEnv, writer, *collection, vid, *dest, *keepLocalDatFile)
	}

	// apply to all volumes in the collection
	// reusing collectVolumeIdsForEcEncode for now
	volumeIds, err := collectVolumeIdsForEcEncode(commandEnv, *collection, *fullPercentage, *quietPeriod)
	if err != nil {
		return err
	}
	fmt.Printf("tier upload volumes: %v\n", volumeIds)
	for _, vid := range volumeIds {
		if err = doVolumeTierUpload(commandEnv, writer, *collection, vid, *dest, *keepLocalDatFile); err != nil {
			return err
		}
	}

	return nil
}

func doVolumeTierUpload(commandEnv *CommandEnv, writer io.Writer, collection string, vid needle.VolumeId, dest string, keepLocalDatFile bool) (err error) {
	// find volume location
	existingLocations, found := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
	if !found {
		return fmt.Errorf("volume %d not found", vid)
	}

	err = markVolumeReplicasWritable(commandEnv.option.GrpcDialOption, vid, existingLocations, false, false)
	if err != nil {
		return fmt.Errorf("mark volume %d as readonly on %s: %v", vid, existingLocations[0].Url, err)
	}

	// copy the .dat file to remote tier
	err = uploadDatToRemoteTier(commandEnv.option.GrpcDialOption, writer, vid, collection, existingLocations[0].ServerAddress(), dest, keepLocalDatFile)
	if err != nil {
		return fmt.Errorf("copy dat file for volume %d on %s to %s: %v", vid, existingLocations[0].Url, dest, err)
	}

	if keepLocalDatFile {
		return nil
	}
	// now the first replica has the .idx and .vif files.
	// ask replicas on other volume server to delete its own local copy
	for i, location := range existingLocations {
		if i == 0 {
			continue
		}
		fmt.Printf("delete volume %d from %s\n", vid, location.Url)
		err = deleteVolume(commandEnv.option.GrpcDialOption, vid, location.ServerAddress(), false)
		if err != nil {
			return fmt.Errorf("deleteVolume %s volume %d: %v", location.Url, vid, err)
		}
	}

	return nil
}

func uploadDatToRemoteTier(grpcDialOption grpc.DialOption, writer io.Writer, volumeId needle.VolumeId, collection string, sourceVolumeServer pb.ServerAddress, dest string, keepLocalDatFile bool) error {

	err := operation.WithVolumeServerClient(true, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		stream, copyErr := volumeServerClient.VolumeTierMoveDatToRemote(context.Background(), &volume_server_pb.VolumeTierMoveDatToRemoteRequest{
			VolumeId:               uint32(volumeId),
			Collection:             collection,
			DestinationBackendName: dest,
			KeepLocalDatFile:       keepLocalDatFile,
		})

		if stream == nil && copyErr == nil {
			// when the volume is already uploaded, VolumeTierMoveDatToRemote will return nil stream and nil error
			// so we should directly return in this case
			fmt.Fprintf(writer, "volume %v already uploaded", volumeId)
			return nil
		}
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

			fmt.Fprintf(writer, "copied %.2f%%, %d bytes, %.2fMB/s\n", resp.ProcessedPercentage, resp.Processed, processingSpeed)

			lastProcessed = resp.Processed
		}

		return copyErr
	})

	return err

}
