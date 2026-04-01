package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandVolumeTierCompact{})
}

type commandVolumeTierCompact struct {
}

func (c *commandVolumeTierCompact) Name() string {
	return "volume.tier.compact"
}

func (c *commandVolumeTierCompact) Help() string {
	return `compact remote volumes to reclaim space on cloud storage

	volume.tier.compact [-volumeId=<volume_id>]
	volume.tier.compact [-collection=""] [-garbageThreshold=0.3]

	e.g.:
	volume.tier.compact -volumeId=7
	volume.tier.compact -collection="mybucket" -garbageThreshold=0.2

	This command compacts cloud tier volumes by:
	1. Downloading the .dat file from remote storage to local
	2. Running compaction to remove deleted data
	3. Uploading the compacted .dat file back to remote storage

	This reclaims space on remote storage that was used by deleted files.

`
}

func (c *commandVolumeTierCompact) HasTag(CommandTag) bool {
	return false
}

type remoteVolumeInfo struct {
	vid               needle.VolumeId
	collection        string
	remoteStorageName string
	serverAddress     pb.ServerAddress
	serverUrl         string
}

func (c *commandVolumeTierCompact) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	tierCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := tierCommand.Int("volumeId", 0, "the volume id")
	collection := tierCommand.String("collection", "", "the collection name (supports regex)")
	garbageThreshold := tierCommand.Float64("garbageThreshold", 0.3, "compact when garbage ratio exceeds this value")
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

	// find remote volumes
	var remoteVolumes []remoteVolumeInfo
	if vid != 0 {
		rv, found, findErr := findRemoteVolumeInTopology(topologyInfo, vid, *collection)
		if findErr != nil {
			return findErr
		}
		if !found {
			return fmt.Errorf("remote volume %d not found", vid)
		}
		remoteVolumes = append(remoteVolumes, rv)
	} else {
		remoteVolumes, err = collectRemoteVolumesWithInfo(topologyInfo, *collection)
		if err != nil {
			return err
		}
	}

	if len(remoteVolumes) == 0 {
		fmt.Fprintf(writer, "no remote volumes found\n")
		return nil
	}

	fmt.Fprintf(writer, "found %d remote volume(s) to check for compaction\n", len(remoteVolumes))

	var failedCount int
	for _, rv := range remoteVolumes {
		if err = doVolumeTierCompact(commandEnv, writer, rv, *garbageThreshold); err != nil {
			fmt.Fprintf(writer, "error compacting volume %d: %v\n", rv.vid, err)
			failedCount++
		}
	}

	if failedCount > 0 {
		return fmt.Errorf("%d of %d volume(s) failed to compact", failedCount, len(remoteVolumes))
	}
	return nil
}

func findRemoteVolumeInTopology(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId, collectionPattern string) (remoteVolumeInfo, bool, error) {
	// when collectionPattern is provided, compile and use as regex filter
	var matchesCollection func(string) bool
	if collectionPattern != "" {
		collectionRegex, err := compileCollectionPattern(collectionPattern)
		if err != nil {
			return remoteVolumeInfo{}, false, fmt.Errorf("invalid collection pattern '%s': %v", collectionPattern, err)
		}
		matchesCollection = collectionRegex.MatchString
	} else {
		matchesCollection = func(string) bool { return true }
	}

	var result remoteVolumeInfo
	found := false
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		if found {
			return
		}
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if needle.VolumeId(v.Id) == vid && v.RemoteStorageName != "" && v.RemoteStorageKey != "" {
					if !matchesCollection(v.Collection) {
						continue
					}
					result = remoteVolumeInfo{
						vid:               vid,
						collection:        v.Collection,
						remoteStorageName: v.RemoteStorageName,
						serverAddress:     pb.NewServerAddressWithGrpcPort(dn.Id, int(dn.GrpcPort)),
						serverUrl:         dn.Id,
					}
					found = true
					return
				}
			}
		}
	})
	return result, found, nil
}

func collectRemoteVolumesWithInfo(topoInfo *master_pb.TopologyInfo, collectionPattern string) ([]remoteVolumeInfo, error) {
	collectionRegex, err := compileCollectionPattern(collectionPattern)
	if err != nil {
		return nil, fmt.Errorf("invalid collection pattern '%s': %v", collectionPattern, err)
	}

	seen := make(map[uint32]bool)
	var result []remoteVolumeInfo
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if v.RemoteStorageName == "" || v.RemoteStorageKey == "" {
					continue
				}
				if !collectionRegex.MatchString(v.Collection) {
					continue
				}
				if seen[v.Id] {
					continue
				}
				seen[v.Id] = true
				result = append(result, remoteVolumeInfo{
					vid:               needle.VolumeId(v.Id),
					collection:        v.Collection,
					remoteStorageName: v.RemoteStorageName,
					serverAddress:     pb.NewServerAddressWithGrpcPort(dn.Id, int(dn.GrpcPort)),
					serverUrl:         dn.Id,
				})
			}
		}
	})

	return result, nil
}

func doVolumeTierCompact(commandEnv *CommandEnv, writer io.Writer, rv remoteVolumeInfo, garbageThreshold float64) error {
	grpcDialOption := commandEnv.option.GrpcDialOption

	// step 1: check garbage level
	garbageRatio, err := checkVolumeGarbage(grpcDialOption, rv.vid, rv.serverAddress)
	if err != nil {
		return fmt.Errorf("check garbage for volume %d: %v", rv.vid, err)
	}

	if garbageRatio < garbageThreshold {
		fmt.Fprintf(writer, "volume %d garbage ratio %.4f below threshold %.4f, skipping\n",
			rv.vid, garbageRatio, garbageThreshold)
		return nil
	}

	fmt.Fprintf(writer, "volume %d garbage ratio %.4f, starting compaction...\n", rv.vid, garbageRatio)

	// step 2: download .dat from remote to local
	// this deletes the remote file and reloads the volume as local
	fmt.Fprintf(writer, "  downloading volume %d from %s to local...\n", rv.vid, rv.remoteStorageName)
	err = downloadDatFromRemoteTier(grpcDialOption, writer, rv.vid, rv.collection, rv.serverAddress)
	if err != nil {
		return fmt.Errorf("download volume %d from remote: %v", rv.vid, err)
	}

	// step 3: compact the local volume
	fmt.Fprintf(writer, "  compacting volume %d...\n", rv.vid)
	err = compactVolumeOnServer(grpcDialOption, writer, rv.vid, rv.serverAddress)
	if err != nil {
		// compaction failed, but volume is now local without remote reference
		// upload the uncompacted volume back to restore cloud tier state
		fmt.Fprintf(writer, "  compaction failed: %v\n", err)
		fmt.Fprintf(writer, "  re-uploading volume %d to %s without compaction...\n", rv.vid, rv.remoteStorageName)
		uploadErr := uploadDatToRemoteTier(grpcDialOption, writer, rv.vid, rv.collection, rv.serverAddress, rv.remoteStorageName, false)
		if uploadErr != nil {
			return fmt.Errorf("compaction failed (%v) and re-upload also failed (%v), volume %d remains local",
				err, uploadErr, rv.vid)
		}
		return fmt.Errorf("compaction failed (%v), volume %d re-uploaded to %s without compaction",
			err, rv.vid, rv.remoteStorageName)
	}

	// step 4: upload compacted volume back to remote
	fmt.Fprintf(writer, "  uploading compacted volume %d to %s...\n", rv.vid, rv.remoteStorageName)
	err = uploadDatToRemoteTier(grpcDialOption, writer, rv.vid, rv.collection, rv.serverAddress, rv.remoteStorageName, false)
	if err != nil {
		return fmt.Errorf("upload compacted volume %d to %s: %v (volume remains local with compacted data)",
			rv.vid, rv.remoteStorageName, err)
	}

	fmt.Fprintf(writer, "volume %d compacted and uploaded to %s successfully\n", rv.vid, rv.remoteStorageName)
	return nil
}

func checkVolumeGarbage(grpcDialOption grpc.DialOption, vid needle.VolumeId, server pb.ServerAddress) (float64, error) {
	var garbageRatio float64
	err := operation.WithVolumeServerClient(false, server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		resp, err := client.VacuumVolumeCheck(context.Background(), &volume_server_pb.VacuumVolumeCheckRequest{
			VolumeId: uint32(vid),
		})
		if err != nil {
			return err
		}
		garbageRatio = resp.GarbageRatio
		return nil
	})
	return garbageRatio, err
}

func compactVolumeOnServer(grpcDialOption grpc.DialOption, writer io.Writer, vid needle.VolumeId, server pb.ServerAddress) error {
	// compact
	err := operation.WithVolumeServerClient(true, server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		stream, err := client.VacuumVolumeCompact(context.Background(), &volume_server_pb.VacuumVolumeCompactRequest{
			VolumeId: uint32(vid),
		})
		if err != nil {
			return err
		}
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				}
				return recvErr
			}
			fmt.Fprintf(writer, "    compacted %d bytes\n", resp.ProcessedBytes)
		}
		return nil
	})
	if err != nil {
		if cleanupErr := vacuumVolumeCleanup(grpcDialOption, vid, server); cleanupErr != nil {
			fmt.Fprintf(writer, "    cleanup after compaction failure also failed: %v\n", cleanupErr)
		}
		return fmt.Errorf("compact: %v", err)
	}

	// commit
	err = operation.WithVolumeServerClient(false, server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.VacuumVolumeCommit(context.Background(), &volume_server_pb.VacuumVolumeCommitRequest{
			VolumeId: uint32(vid),
		})
		return err
	})
	if err != nil {
		if cleanupErr := vacuumVolumeCleanup(grpcDialOption, vid, server); cleanupErr != nil {
			fmt.Fprintf(writer, "    cleanup after commit failure also failed: %v\n", cleanupErr)
		}
		return fmt.Errorf("commit: %v", err)
	}

	return nil
}

func vacuumVolumeCleanup(grpcDialOption grpc.DialOption, vid needle.VolumeId, server pb.ServerAddress) error {
	return operation.WithVolumeServerClient(false, server, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.VacuumVolumeCleanup(context.Background(), &volume_server_pb.VacuumVolumeCleanupRequest{
			VolumeId: uint32(vid),
		})
		return err
	})
}
