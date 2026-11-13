package shell

import (
	"context"
	"errors"
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
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
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
	that replica over every other replica, and only then deletes the remote object. The object is
	kept while fewer replicas are listed than the replica placement expects; rerun the command once
	they are back. A replica on an offline volume server that the placement does not account for
	keeps referencing the deleted object: delete that replica and run volume.fix.replication.

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
		return err
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
func downloadAndReplicateFromRemoteTier(grpcDialOption grpc.DialOption, writer io.Writer, readTopology func() (*master_pb.TopologyInfo, error), vid needle.VolumeId, collection string, concurrency int) (err error) {
	topoInfo, err := readTopology()
	if err != nil {
		return err
	}
	remote, local, copies := collectVolumeTierDownloadLocations(topoInfo, vid, collection)
	listed := len(remote) + len(local)
	if len(remote) == 0 {
		if len(local) == 0 {
			return fmt.Errorf("volume %d is not listed on any volume server", vid)
		}
		fmt.Fprintf(writer, "volume %d is already on local disk\n", vid)
		return nil
	}

	var source pb.ServerAddress
	fresh := true
	if len(local) > 0 {
		// a previous run already downloaded this replica
		source = local[0].server
	} else {
		source, remote, fresh = remote[0].server, remote[1:], false
		// a lone replica is the only reference, so it may delete the object itself
		keepRemote := len(remote) > 0 || listed < copies
		if err = downloadDatFromRemoteTier(grpcDialOption, writer, vid, collection, source, keepRemote, concurrency); err != nil {
			if !strings.Contains(err.Error(), "already on local disk") {
				return fmt.Errorf("download dat file for volume %d to %s: %v", vid, source, err)
			}
			fmt.Fprintf(writer, "volume %d on %s is already on local disk\n", vid, source)
		}
		if len(remote) == 0 {
			if keepRemote {
				fmt.Fprintf(writer, "volume %d: only %d of %d replicas are listed, the remote object is kept\n", vid, listed, copies)
			}
			return nil
		}
	}

	restore, err := freezeTierDownloadSource(grpcDialOption, vid, source)
	if restore != nil {
		defer func() {
			if restoreErr := restore(); restoreErr != nil {
				err = errors.Join(err, fmt.Errorf("restore volume %d state on %s: %w", vid, source, restoreErr))
			}
		}()
	}
	if err != nil {
		return fmt.Errorf("block deletes on volume %d at %s: %v", vid, source, err)
	}

	// Copy over all remote-backed replicas but one, re-reading the topology
	// so a replica that was offline or created meanwhile is replaced too. The
	// last one is deleted together with the remote object and then replaced.
	replaced := map[pb.ServerAddress]bool{source: true}
	for {
		var pending []tierDownloadReplica
		for _, replica := range remote {
			if !replaced[replica.server] {
				pending = append(pending, replica)
			}
		}
		if len(pending) == 0 {
			fmt.Fprintf(writer, "volume %d: no remote-backed replica is listed any more, the remote object is kept\n", vid)
			return nil
		}
		if len(pending) == 1 && fresh {
			if listed < copies {
				fmt.Fprintf(writer, "volume %d: only %d of %d replicas are listed, the remote object is kept\n", vid, listed, copies)
				return nil
			}
			if server, found := serverListedTwice(remote, local); found {
				fmt.Fprintf(writer, "volume %d is listed twice on %s, the remote object is kept\n", vid, server)
				return nil
			}
			last := pending[0]
			fmt.Fprintf(writer, "delete remote-backed replica of volume %d on %s with its remote object\n", vid, last.server)
			if err = deleteVolume(context.Background(), grpcDialOption, vid, last.server, false, false, false); err != nil {
				return fmt.Errorf("delete volume %d with its remote object on %s: %v; if the delete was applied, run volume.fix.replication to restore the replica", vid, last.server, err)
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
			replaced[target.server] = true
		}
		if topoInfo, err = readTopology(); err != nil {
			return err
		}
		remote, local, copies = collectVolumeTierDownloadLocations(topoInfo, vid, collection)
		listed, fresh = len(remote)+len(local), true
	}
}

// freezeTierDownloadSource blocks deletes on the copy source: a delete applied
// after VolumeCopy read the source index would be lost on the replaced replica.
// restore brings back the writable mark, or reloads the persisted state. It is
// also returned when the mark fails, since the server may have applied it.
func freezeTierDownloadSource(grpcDialOption grpc.DialOption, vid needle.VolumeId, source pb.ServerAddress) (restore func() error, err error) {
	var wasWritable, statusRead bool
	err = operation.WithVolumeServerClient(false, source, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		resp, statusErr := client.VolumeStatus(context.Background(), &volume_server_pb.VolumeStatusRequest{VolumeId: uint32(vid)})
		if statusErr != nil {
			return statusErr
		}
		wasWritable, statusRead = !resp.IsReadOnly, true
		_, markErr := client.VolumeMarkReadonly(context.Background(), &volume_server_pb.VolumeMarkReadonlyRequest{VolumeId: uint32(vid)})
		return markErr
	})
	if !statusRead {
		return nil, err
	}
	restore = func() error {
		return operation.WithVolumeServerClient(false, source, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			if wasWritable {
				_, markErr := client.VolumeMarkWritable(context.Background(), &volume_server_pb.VolumeMarkWritableRequest{VolumeId: uint32(vid)})
				return markErr
			}
			return remountVolume(client, vid)
		})
	}
	return
}

func remountVolume(client volume_server_pb.VolumeServerClient, vid needle.VolumeId) error {
	if _, err := client.VolumeUnmount(context.Background(), &volume_server_pb.VolumeUnmountRequest{VolumeId: uint32(vid)}); err != nil {
		return err
	}
	_, err := client.VolumeMount(context.Background(), &volume_server_pb.VolumeMountRequest{VolumeId: uint32(vid)})
	return err
}

func replicateTierDownloadedVolume(grpcDialOption grpc.DialOption, writer io.Writer, vid needle.VolumeId, source pb.ServerAddress, target tierDownloadReplica) error {
	fmt.Fprintf(writer, "replicate downloaded volume %d from %s to %s\n", vid, source, target.server)
	if _, err := copyVolume(context.Background(), grpcDialOption, writer, vid, source, target.server, types.ToDiskType(target.diskType).ReadableString(), 0, true); err != nil {
		return fmt.Errorf("replicate volume %d from %s to %s: %v", vid, source, target.server, err)
	}
	return nil
}

func serverListedTwice(remote, local []tierDownloadReplica) (pb.ServerAddress, bool) {
	seen := make(map[pb.ServerAddress]bool)
	for _, replica := range append(append([]tierDownloadReplica{}, remote...), local...) {
		if seen[replica.server] {
			return replica.server, true
		}
		seen[replica.server] = true
	}
	return "", false
}

type tierDownloadReplica struct {
	server   pb.ServerAddress
	diskType string
}

// collectVolumeTierDownloadLocations splits the replicas of a volume into the
// ones still reading the remote object and the ones already on local disk, and
// reports how many replicas the replica placement expects.
func collectVolumeTierDownloadLocations(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId, collection string) (remote, local []tierDownloadReplica, copies int) {
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, disk := range dn.DiskInfos {
			for _, vi := range disk.VolumeInfos {
				if needle.VolumeId(vi.Id) != vid || vi.Collection != collection {
					continue
				}
				if rp, rpErr := super_block.NewReplicaPlacementFromByte(byte(vi.ReplicaPlacement)); rpErr == nil {
					copies = max(copies, rp.GetCopyCount())
				}
				replica := tierDownloadReplica{server: pb.NewServerAddressFromDataNode(dn), diskType: vi.DiskType}
				if vi.RemoteStorageName != "" {
					remote = append(remote, replica)
				} else {
					local = append(local, replica)
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

		return remountVolume(volumeServerClient, volumeId)
	})

	return err

}
