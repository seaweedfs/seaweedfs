package shell

import (
	"context"
	"flag"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/operation/volume_move"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
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

	volume.tier.compact [-volumeId=<volume_id>] [-concurrency=<n>]
	volume.tier.compact [-collection=""] [-garbageThreshold=0.3] [-concurrency=<n>]

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
	collection := tierCommand.String("collection", "", "comma-separated collection names, wildcards, or regex patterns; empty matches the collection with no name")
	garbageThreshold := tierCommand.Float64("garbageThreshold", 0.3, "compact when garbage ratio exceeds this value")
	concurrency := tierCommand.Int("concurrency", 0, "multipart transfer concurrency (0 = backend default)")
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
		if err = doVolumeTierCompact(commandEnv, writer, topologyInfo, rv, *garbageThreshold, *concurrency); err != nil {
			fmt.Fprintf(writer, "error compacting volume %d: %v\n", rv.vid, err)
			failedCount++
		}
	}

	if failedCount > 0 {
		return fmt.Errorf("%d of %d volume(s) failed to compact", failedCount, len(remoteVolumes))
	}
	return nil
}

// findVolumeCollection resolves the collection a volume actually lives in,
// honoring the -collection filter: an empty pattern matches any collection, a
// non-empty one compiles like the rest of the tier commands. The volume server
// verifies the collection name on tier requests, so callers need the real
// name, not the pattern.
func findVolumeCollection(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId, collectionPattern string) (collection string, found bool, err error) {
	var matchesCollection func(string) bool
	if collectionPattern != "" {
		collectionMatcher, compileErr := compileCollectionPattern(collectionPattern)
		if compileErr != nil {
			return "", false, fmt.Errorf("invalid collection pattern '%s': %v", collectionPattern, compileErr)
		}
		matchesCollection = collectionMatcher.Matches
	} else {
		matchesCollection = func(string) bool { return true }
	}

	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		if found {
			return
		}
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if needle.VolumeId(v.Id) == vid && matchesCollection(v.Collection) {
					collection, found = v.Collection, true
					return
				}
			}
		}
	})
	return
}

func findRemoteVolumeInTopology(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId, collectionPattern string) (remoteVolumeInfo, bool, error) {
	var matchesCollection func(string) bool
	if collectionPattern != "" {
		collectionMatcher, err := compileCollectionPattern(collectionPattern)
		if err != nil {
			return remoteVolumeInfo{}, false, fmt.Errorf("invalid collection pattern '%s': %v", collectionPattern, err)
		}
		matchesCollection = collectionMatcher.Matches
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
				if needle.VolumeId(v.Id) == vid && v.RemoteStorageName != "" {
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
	collectionMatcher, err := compileCollectionPattern(collectionPattern)
	if err != nil {
		return nil, fmt.Errorf("invalid collection pattern '%s': %v", collectionPattern, err)
	}

	seen := make(map[uint32]bool)
	var result []remoteVolumeInfo
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				if v.RemoteStorageName == "" {
					continue
				}
				if !collectionMatcher.Matches(v.Collection) {
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

// collectVolumeTierCompactLocations returns the volume's tiered replicas first
// followed by any local ones. Every tiered replica references the same remote
// object, so the source is always a tiered replica and the last tiered replica
// doubles as the one that can retire the old object after the resync.
func collectVolumeTierCompactLocations(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId, collection string) (tiered, local []wdclient.Location) {
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, disk := range dn.DiskInfos {
			for _, vi := range disk.VolumeInfos {
				if needle.VolumeId(vi.Id) != vid || vi.Collection != collection {
					continue
				}
				loc := wdclient.Location{
					Url:        dn.Id,
					PublicUrl:  dn.Id,
					GrpcPort:   int(dn.GrpcPort),
					DataCenter: string(dc),
				}
				if vi.RemoteStorageName != "" {
					tiered = append(tiered, loc)
				} else {
					local = append(local, loc)
				}
			}
		}
	})
	return
}

func doVolumeTierCompact(commandEnv *CommandEnv, writer io.Writer, topoInfo *master_pb.TopologyInfo, rv remoteVolumeInfo, garbageThreshold float64, concurrency int) error {
	grpcDialOption := commandEnv.option.GrpcDialOption
	ctx := context.Background()

	tieredLocations, localLocations := collectVolumeTierCompactLocations(topoInfo, rv.vid, rv.collection)
	if len(tieredLocations) == 0 {
		return fmt.Errorf("volume %d has no remote replica", rv.vid)
	}
	source := tieredLocations[0].ServerAddress()
	// all replicas of the volume: tiered ones share one remote object, and any
	// local copy is re-pointed at the new object during the resync below
	locations := append(append([]wdclient.Location{}, tieredLocations...), localLocations...)

	// step 1: check garbage level
	garbageRatio, err := checkVolumeGarbage(grpcDialOption, rv.vid, source)
	if err != nil {
		return fmt.Errorf("check garbage for volume %d: %v", rv.vid, err)
	}

	if garbageRatio < garbageThreshold {
		fmt.Fprintf(writer, "volume %d garbage ratio %.4f below threshold %.4f, skipping\n",
			rv.vid, garbageRatio, garbageThreshold)
		return nil
	}

	fmt.Fprintf(writer, "volume %d garbage ratio %.4f, starting compaction...\n", rv.vid, garbageRatio)

	// step 2: freeze every replica. The mark is persisted because the download
	// remounts the volume into a fresh in-memory state, and a local .dat with no
	// persisted readonly flag would load writable while its siblings stay readonly.
	frozen, err := markVolumeReplicasWritable(ctx, grpcDialOption, rv.vid, locations, false, true)
	if err != nil {
		// nothing changed yet: thaw the replicas already marked
		if _, thawErr := markVolumeReplicasWritable(ctx, grpcDialOption, rv.vid, frozen, true, false); thawErr != nil {
			return fmt.Errorf("mark volume %d readonly: %v (thawing the %d marked replicas also failed: %v)",
				rv.vid, err, len(frozen), thawErr)
		}
		return fmt.Errorf("mark volume %d readonly: %v", rv.vid, err)
	}
	// Past this point a failure keeps every replica frozen on purpose: a replica
	// mid-transition may hold divergent data, and thawing it would expose that.
	// Re-running volume.tier.compact completes the resync and releases the marks.
	stillFrozen := func(stageErr error) error {
		return fmt.Errorf("%v (volume %d replicas stay readonly; rerun volume.tier.compact to resume)", stageErr, rv.vid)
	}

	// step 3: download .dat from remote to local; the shared object stays while
	// sibling replicas still reference it
	fmt.Fprintf(writer, "  downloading volume %d from %s to local...\n", rv.vid, rv.remoteStorageName)
	err = downloadDatFromRemoteTier(grpcDialOption, writer, rv.vid, rv.collection, source, len(tieredLocations) > 1, concurrency)
	if err != nil {
		return stillFrozen(fmt.Errorf("download volume %d from remote: %v", rv.vid, err))
	}

	// step 4: compact the local volume; on failure re-upload what is there so
	// the replica returns to the tier and the resync below can still proceed
	compactErr := compactVolumeOnServer(grpcDialOption, writer, rv.vid, source)
	if compactErr != nil {
		fmt.Fprintf(writer, "  compaction failed: %v\n", compactErr)
		fmt.Fprintf(writer, "  re-uploading volume %d to %s without compaction...\n", rv.vid, rv.remoteStorageName)
	}

	// step 5: upload the (possibly uncompacted) volume back to remote under a new key
	fmt.Fprintf(writer, "  uploading volume %d to %s...\n", rv.vid, rv.remoteStorageName)
	uploadErr := uploadDatToRemoteTier(grpcDialOption, writer, rv.vid, rv.collection, source, rv.remoteStorageName, false, concurrency)
	if uploadErr != nil {
		if compactErr != nil {
			return stillFrozen(fmt.Errorf("compaction failed (%v) and re-upload also failed (%v), volume %d remains local",
				compactErr, uploadErr, rv.vid))
		}
		return stillFrozen(fmt.Errorf("upload compacted volume %d to %s: %v (volume remains local with compacted data)",
			rv.vid, rv.remoteStorageName, uploadErr))
	}

	// step 6: push the new .idx/.vif to every sibling so all replicas reference
	// the new remote object; the last tiered replica goes last so the old shared
	// object still has a holder until then. Replicas stay frozen until every
	// copy lands, so a delete cannot slip past a sibling's index snapshot; a
	// copied readonly mark is inert on a remote-backed volume anyway.
	syncErr := syncVolumeTierCompactReplicas(ctx, grpcDialOption, writer, rv, tieredLocations, localLocations)
	if compactErr != nil {
		if syncErr != nil {
			return stillFrozen(fmt.Errorf("compaction failed (%v), volume %d re-uploaded without compaction but resync failed: %v",
				compactErr, rv.vid, syncErr))
		}
		return fmt.Errorf("compaction failed (%v), volume %d re-uploaded to %s without compaction",
			compactErr, rv.vid, rv.remoteStorageName)
	}
	if syncErr != nil {
		return stillFrozen(syncErr)
	}

	// step 7: unfreeze every replica, clearing the persisted readonly mark on
	// each. Remote volumes refuse writes regardless; this re-enables deletes.
	thawed, err := markVolumeReplicasWritable(ctx, grpcDialOption, rv.vid, locations, true, false)
	if err != nil {
		return fmt.Errorf("unfreeze volume %d replicas: %v (%d of %d released)", rv.vid, err, len(thawed), len(locations))
	}

	fmt.Fprintf(writer, "volume %d compacted and uploaded to %s successfully\n", rv.vid, rv.remoteStorageName)
	return nil
}

// planTierCompactResync orders the sibling resyncs. The last tiered replica is
// held out for last: the resync retires a remote object only once its final
// holder is re-pointed, so processing the old object's expected last holder at
// the end keeps it alive for every sibling still referencing it. A
// single-replica volume has no such holder: its download already removed the
// object.
func planTierCompactResync(tieredLocations, localLocations []wdclient.Location) (order, deleter []wdclient.Location) {
	rest := append(append([]wdclient.Location{}, tieredLocations[1:]...), localLocations...)
	if len(tieredLocations) > 1 {
		last := tieredLocations[len(tieredLocations)-1]
		for _, loc := range rest {
			if loc.ServerAddress() == last.ServerAddress() {
				deleter = append(deleter, loc)
				continue
			}
			order = append(order, loc)
		}
		return
	}
	return rest, nil
}

func syncVolumeTierCompactReplicas(ctx context.Context, grpcDialOption grpc.DialOption, writer io.Writer, rv remoteVolumeInfo, tieredLocations, localLocations []wdclient.Location) error {
	source := tieredLocations[0].ServerAddress()
	mover := volume_move.NewMover(grpcDialOption)

	resync := func(loc wdclient.Location) error {
		fmt.Fprintf(writer, "  replicating volume %d metadata from %s to %s\n", rv.vid, tieredLocations[0].Url, loc.Url)
		return replicateVolumeToServer(ctx, grpcDialOption, writer, rv.vid, source, loc.ServerAddress(), "", 0)
	}

	order, deleter := planTierCompactResync(tieredLocations, localLocations)

	// Every replica's current remote objects, so a resynced-over replica can
	// retire an object once it is the last holder — covers the shared object
	// and any object a previous interrupted run left referenced. The source
	// counts as a holder of the new object so a replica already sharing it is
	// never mistaken for the last holder.
	allLocations := append(append([]wdclient.Location{}, tieredLocations...), localLocations...)
	holders, readErr := readRemoteObjectHolders(ctx, grpcDialOption, rv.vid, allLocations)
	if readErr != nil {
		// Fall back to the explicit last-replica deleter when a replica's
		// remote files cannot be read.
		for _, loc := range order {
			if err := resync(loc); err != nil {
				return fmt.Errorf("replicate volume %d from %s to %s: %v", rv.vid, tieredLocations[0].Url, loc.Url, err)
			}
		}
		if len(deleter) == 0 {
			return nil
		}
		fmt.Fprintf(writer, "  removing old remote copy of volume %d on %s\n", rv.vid, deleter[0].Url)
		if err := mover.DeleteVolume(ctx, rv.vid, deleter[0].ServerAddress(), false, false, false); err != nil {
			return fmt.Errorf("delete old copy of volume %d on %s: %v", rv.vid, deleter[0].Url, err)
		}
		return resync(deleter[0])
	}

	for _, loc := range append(append([]wdclient.Location{}, order...), deleter...) {
		files := holders.files[loc.ServerAddress()]
		soleHolder := len(files) > 0
		for _, f := range files {
			if holders.count[remoteFileKey(f)] > 1 {
				soleHolder = false
				break
			}
		}
		if soleHolder {
			// This replica is the only holder of its remote objects: removing
			// its copy retires them, and the resync restores the replica on the
			// new object.
			fmt.Fprintf(writer, "  removing old remote copy of volume %d on %s\n", rv.vid, loc.Url)
			if err := mover.DeleteVolume(ctx, rv.vid, loc.ServerAddress(), false, false, false); err != nil {
				return fmt.Errorf("delete old copy of volume %d on %s: %v", rv.vid, loc.Url, err)
			}
		}
		for _, f := range files {
			holders.count[remoteFileKey(f)]--
		}
		if err := resync(loc); err != nil {
			return fmt.Errorf("replicate volume %d from %s to %s: %v", rv.vid, tieredLocations[0].Url, loc.Url, err)
		}
	}
	return nil
}

// remoteObjectHolders reads every replica's remote file list and counts how
// many replicas reference each remote object.
type remoteObjectHolders struct {
	files map[pb.ServerAddress][]*volume_server_pb.RemoteFile
	count map[string]int
}

func remoteFileKey(f *volume_server_pb.RemoteFile) string {
	return f.BackendType + "/" + f.BackendId + "/" + f.Key
}

func readRemoteObjectHolders(ctx context.Context, grpcDialOption grpc.DialOption, vid needle.VolumeId, locations []wdclient.Location) (*remoteObjectHolders, error) {
	h := &remoteObjectHolders{
		files: make(map[pb.ServerAddress][]*volume_server_pb.RemoteFile),
		count: make(map[string]int),
	}
	for _, loc := range locations {
		addr := loc.ServerAddress()
		err := operation.WithVolumeServerClient(false, addr, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			resp, reqErr := client.ReadVolumeFileStatus(ctx, &volume_server_pb.ReadVolumeFileStatusRequest{
				VolumeId: uint32(vid),
			})
			if reqErr != nil {
				return reqErr
			}
			if resp == nil || resp.VolumeInfo == nil {
				return fmt.Errorf("volume %d status has no remote file info", vid)
			}
			h.files[addr] = resp.VolumeInfo.GetFiles()
			return nil
		})
		if err != nil {
			return nil, err
		}
		for _, f := range h.files[addr] {
			h.count[remoteFileKey(f)]++
		}
	}
	return h, nil
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
