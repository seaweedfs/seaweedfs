package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

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
	volume.tier.download [-collection=""] -volumeId=<volume_id> [-concurrency=<n>]

	The -collection parameter supports regular expressions for pattern matching:
	  - Use exact match: volume.tier.download -collection="^mybucket$"
	  - Match multiple buckets: volume.tier.download -collection="bucket.*"
	  - Match all collections: volume.tier.download -collection=".*"

	e.g.:
	volume.tier.download -volumeId=7
	volume.tier.download -volumeId=7 -concurrency=1

	This command downloads the dat file of a volume from a remote tier onto one replica, copies
	that replica over every other replica, and only then deletes the remote object. A replica on a
	volume server that is offline during the command keeps referencing the deleted object.

`
}

func (c *commandVolumeTierDownload) HasTag(CommandTag) bool {
	return false
}

func (c *commandVolumeTierDownload) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	tierCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := tierCommand.Int("volumeId", 0, "the volume id")
	collection := tierCommand.String("collection", "", "comma-separated collection names, wildcards, or regex patterns; empty matches the collection with no name")
	concurrency := tierCommand.Int("concurrency", 0, "multipart download concurrency (0 = backend default)")
	if err = tierCommand.Parse(args); err != nil {
		return nil
	}

	if err = validateTierConcurrency(*concurrency); err != nil {
		return err
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
		return doVolumeTierDownload(commandEnv, writer, topologyInfo, *collection, vid, *concurrency)
	}

	// apply to all volumes in the collection
	// reusing collectVolumeIdsForEcEncode for now
	volumeIds, err := collectRemoteVolumes(topologyInfo, *collection)
	if err != nil {
		return err
	}
	fmt.Printf("tier download volumes: %v\n", volumeIds)
	for _, vid := range volumeIds {
		if err = doVolumeTierDownload(commandEnv, writer, topologyInfo, *collection, vid, *concurrency); err != nil {
			return err
		}
	}

	return nil
}

func collectRemoteVolumes(topoInfo *master_pb.TopologyInfo, collectionPattern string) (vids []needle.VolumeId, err error) {
	// compile regex pattern for collection matching
	collectionMatcher, err := compileCollectionPattern(collectionPattern)
	if err != nil {
		return nil, fmt.Errorf("invalid collection pattern '%s': %v", collectionPattern, err)
	}

	vidMap := make(map[uint32]bool)
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if collectionMatcher.Matches(v.Collection) && v.RemoteStorageName != "" {
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

func doVolumeTierDownload(commandEnv *CommandEnv, writer io.Writer, topoInfo *master_pb.TopologyInfo, collectionPattern string, vid needle.VolumeId, concurrency int) (err error) {
	// The volume server verifies the request's collection against the volume's
	// own, so the pattern must resolve to the actual collection here.
	collection, found, err := findVolumeCollection(topoInfo, vid, collectionPattern)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("volume %d not found in collections matching %q", vid, collectionPattern)
	}
	readTopology := func() (*master_pb.TopologyInfo, error) {
		topo, _, topoErr := collectTopologyInfo(commandEnv, 0)
		return topo, topoErr
	}
	return downloadAndReplicateFromRemoteTier(commandEnv.option.GrpcDialOption, writer, readTopology, vid, collection, concurrency)
}

// downloadAndReplicateFromRemoteTier downloads the shared remote object onto
// one replica, copies that local volume over every other replica, and only
// then deletes the remote object, so no replica is ever left referencing a
// deleted object.
func downloadAndReplicateFromRemoteTier(grpcDialOption grpc.DialOption, writer io.Writer, readTopology func() (*master_pb.TopologyInfo, error), vid needle.VolumeId, collection string, concurrency int) error {
	topoInfo, err := readTopology()
	if err != nil {
		return err
	}
	remote, local := collectVolumeTierDownloadLocations(topoInfo, vid, collection)
	if len(remote) == 0 {
		fmt.Fprintf(writer, "volume %d is already on local disk\n", vid)
		return nil
	}

	source, fresh := pb.ServerAddress(""), true
	if len(local) > 0 {
		// a previous run already downloaded this replica
		source = local[0]
	} else {
		source, remote, fresh = remote[0], remote[1:], false
		// a lone replica is the only reference, so it may delete the object itself
		keepRemote := len(remote) > 0
		if err = downloadDatFromRemoteTier(grpcDialOption, writer, vid, collection, source, keepRemote, concurrency); err != nil {
			if !strings.Contains(err.Error(), "already on local disk") {
				return fmt.Errorf("download dat file for volume %d to %s: %v", vid, source, err)
			}
			fmt.Fprintf(writer, "volume %d on %s is already on local disk\n", vid, source)
		}
		if !keepRemote {
			return nil
		}
	}

	// Copy over all remote-backed replicas but one, re-reading the topology
	// so a replica that was offline or created meanwhile is replaced too. The
	// last one is deleted together with the remote object and then replaced.
	replaced := map[pb.ServerAddress]bool{source: true}
	for {
		var pending []pb.ServerAddress
		for _, server := range remote {
			if !replaced[server] {
				pending = append(pending, server)
			}
		}
		if len(pending) == 0 {
			fmt.Fprintf(writer, "volume %d: no remote-backed replica is listed any more, the remote object is kept\n", vid)
			return nil
		}
		if len(pending) == 1 && fresh {
			last := pending[0]
			fmt.Fprintf(writer, "delete remote-backed replica of volume %d on %s with its remote object\n", vid, last)
			if err = deleteVolume(context.Background(), grpcDialOption, vid, last, false, false, false); err != nil {
				return fmt.Errorf("delete volume %d with its remote object on %s: %v", vid, last, err)
			}
			if err = replicateTierDownloadedVolume(grpcDialOption, writer, vid, source, last); err != nil {
				return fmt.Errorf("%v; run volume.fix.replication to restore the replica", err)
			}
			return nil
		}
		for _, target := range pending[:len(pending)-1] {
			if err = replicateTierDownloadedVolume(grpcDialOption, writer, vid, source, target); err != nil {
				return err
			}
			replaced[target] = true
		}
		if topoInfo, err = readTopology(); err != nil {
			return err
		}
		remote, _ = collectVolumeTierDownloadLocations(topoInfo, vid, collection)
		fresh = true
	}
}

func replicateTierDownloadedVolume(grpcDialOption grpc.DialOption, writer io.Writer, vid needle.VolumeId, source, target pb.ServerAddress) error {
	fmt.Fprintf(writer, "replicate downloaded volume %d from %s to %s\n", vid, source, target)
	if _, err := copyVolume(context.Background(), grpcDialOption, writer, vid, source, target, "", 0, true); err != nil {
		return fmt.Errorf("replicate volume %d from %s to %s: %v", vid, source, target, err)
	}
	return nil
}

// collectVolumeTierDownloadLocations splits the replicas of a volume into the
// ones still reading the remote object and the ones already on local disk.
func collectVolumeTierDownloadLocations(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId, collection string) (remote, local []pb.ServerAddress) {
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, disk := range dn.DiskInfos {
			for _, vi := range disk.VolumeInfos {
				if needle.VolumeId(vi.Id) != vid || vi.Collection != collection {
					continue
				}
				if vi.RemoteStorageName != "" {
					remote = append(remote, pb.NewServerAddressFromDataNode(dn))
				} else {
					local = append(local, pb.NewServerAddressFromDataNode(dn))
				}
			}
		}
	})
	return
}

func downloadDatFromRemoteTier(grpcDialOption grpc.DialOption, writer io.Writer, volumeId needle.VolumeId, collection string, targetVolumeServer pb.ServerAddress, keepRemote bool, concurrency int) error {

	err := operation.WithVolumeServerClient(true, targetVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		stream, downloadErr := volumeServerClient.VolumeTierMoveDatFromRemote(context.Background(), &volume_server_pb.VolumeTierMoveDatFromRemoteRequest{
			VolumeId:          uint32(volumeId),
			Collection:        collection,
			KeepRemoteDatFile: keepRemote,
			Concurrency:       int32(concurrency),
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
