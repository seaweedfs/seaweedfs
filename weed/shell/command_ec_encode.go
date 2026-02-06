package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

func init() {
	Commands = append(Commands, &commandEcEncode{})
}

type commandEcEncode struct {
}

func (c *commandEcEncode) Name() string {
	return "ec.encode"
}

func (c *commandEcEncode) Help() string {
	return `apply erasure coding to a volume

	ec.encode [-collection=""] [-fullPercent=95 -quietFor=1h] [-verbose] [-sourceDiskType=<disk_type>] [-diskType=<disk_type>]
	ec.encode [-collection=""] [-volumeId=<volume_id>] [-verbose] [-diskType=<disk_type>]

	This command will:
	1. freeze one volume
	2. apply erasure coding to the volume
	3. (optionally) re-balance encoded shards across multiple volume servers

	The erasure coding is 10.4. So ideally you have more than 14 volume servers, and you can afford
	to lose 4 volume servers.

	If the number of volumes are not high, the worst case is that you only have 4 volume servers,
	and the shards are spread as 4,4,3,3, respectively. You can afford to lose one volume server.

	If you only have less than 4 volume servers, with erasure coding, at least you can afford to
	have 4 corrupted shard files.

	The -collection parameter supports regular expressions for pattern matching:
	  - Use exact match: ec.encode -collection="^mybucket$"
	  - Match multiple buckets: ec.encode -collection="bucket.*"
	  - Match all collections: ec.encode -collection=".*"

	Options:
	  -verbose: show detailed reasons why volumes are not selected for encoding
	  -sourceDiskType: filter source volumes by disk type (hdd, ssd, or empty for all)
	  -diskType: target disk type for EC shards (hdd, ssd, or empty for default hdd)

	Examples:
	  # Encode SSD volumes to SSD EC shards (same tier)
	  ec.encode -collection=mybucket -sourceDiskType=ssd -diskType=ssd

	  # Encode SSD volumes to HDD EC shards (tier migration to cheaper storage)
	  ec.encode -collection=mybucket -sourceDiskType=ssd -diskType=hdd

	  # Encode all volumes to SSD EC shards
	  ec.encode -collection=mybucket -diskType=ssd

	Re-balancing algorithm:
	` + ecBalanceAlgorithmDescription
}

func (c *commandEcEncode) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcEncode) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	encodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := encodeCommand.Int("volumeId", 0, "the volume id")
	collection := encodeCommand.String("collection", "", "the collection name")
	fullPercentage := encodeCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := encodeCommand.Duration("quietFor", time.Hour, "select volumes without no writes for this period")
	maxParallelization := encodeCommand.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")
	forceChanges := encodeCommand.Bool("force", false, "force the encoding even if the cluster has less than recommended 4 nodes")
	shardReplicaPlacement := encodeCommand.String("shardReplicaPlacement", "", "replica placement for EC shards, or master default if empty")
	sourceDiskTypeStr := encodeCommand.String("sourceDiskType", "", "filter source volumes by disk type (hdd, ssd, or empty for all)")
	diskTypeStr := encodeCommand.String("diskType", "", "target disk type for EC shards (hdd, ssd, or empty for default hdd)")
	applyBalancing := encodeCommand.Bool("rebalance", true, "re-balance EC shards after creation (default: true)")
	verbose := encodeCommand.Bool("verbose", false, "show detailed reasons why volumes are not selected for encoding")

	if err = encodeCommand.Parse(args); err != nil {
		return nil
	}
	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}
	rp, err := parseReplicaPlacementArg(commandEnv, *shardReplicaPlacement)
	if err != nil {
		return err
	}

	// Parse source disk type filter (optional)
	var sourceDiskType *types.DiskType
	if *sourceDiskTypeStr != "" {
		sdt := types.ToDiskType(*sourceDiskTypeStr)
		sourceDiskType = &sdt
	}

	// Parse target disk type for EC shards
	diskType := types.ToDiskType(*diskTypeStr)

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	if !*forceChanges {
		var nodeCount int
		eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
			nodeCount++
		})
		if nodeCount < erasure_coding.ParityShardsCount {
			glog.V(0).Infof("skip erasure coding with %d nodes, less than recommended %d nodes", nodeCount, erasure_coding.ParityShardsCount)
			return nil
		}
	}

	var volumeIds []needle.VolumeId
	var balanceCollections []string
	if vid := needle.VolumeId(*volumeId); vid != 0 {
		// volumeId is provided
		volumeIds = append(volumeIds, vid)
		balanceCollections = collectCollectionsForVolumeIds(topologyInfo, volumeIds)
	} else {
		// apply to all volumes for the given collection pattern (regex)
		volumeIds, balanceCollections, err = collectVolumeIdsForEcEncode(commandEnv, *collection, sourceDiskType, *fullPercentage, *quietPeriod, *verbose)
		if err != nil {
			return err
		}
	}
	if len(volumeIds) == 0 {
		fmt.Println("No volumes, nothing to do.")
		return nil
	}

	// Collect volume ID to collection name mapping for the sync operation
	volumeIdToCollection := collectVolumeIdToCollection(topologyInfo, volumeIds)

	// Collect volume locations BEFORE EC encoding starts to avoid race condition
	// where the master metadata is updated after EC encoding but before deletion
	fmt.Printf("Collecting volume locations for %d volumes before EC encoding...\n", len(volumeIds))
	volumeLocationsMap, err := volumeLocations(commandEnv, volumeIds)
	if err != nil {
		return fmt.Errorf("failed to collect volume locations before EC encoding: %w", err)
	}

	// Pre-flight check: verify the target disk type has capacity for EC shards
	// This prevents encoding shards only to fail during rebalance
	_, totalFreeEcSlots, err := collectEcNodesForDC(commandEnv, "", diskType)
	if err != nil {
		return fmt.Errorf("failed to check EC shard capacity: %w", err)
	}

	// Calculate required slots: each volume needs TotalShardsCount (14) shards distributed
	requiredSlots := len(volumeIds) * erasure_coding.TotalShardsCount
	if totalFreeEcSlots < 1 {
		// No capacity at all on the target disk type
		if diskType != types.HardDriveType {
			return fmt.Errorf("no free ec shard slots on disk type '%s'. The target disk type has no capacity.\n"+
				"Your volumes are likely on a different disk type. Try:\n"+
				"  ec.encode -collection=%s -diskType=hdd\n"+
				"Or omit -diskType to use the default (hdd)", diskType, *collection)
		}
		return fmt.Errorf("no free ec shard slots. only %d left on disk type '%s'", totalFreeEcSlots, diskType)
	}

	if totalFreeEcSlots < requiredSlots {
		fmt.Printf("Warning: limited EC shard capacity. Need %d slots for %d volumes, but only %d slots available on disk type '%s'.\n",
			requiredSlots, len(volumeIds), totalFreeEcSlots, diskType)
		fmt.Printf("Rebalancing may not achieve optimal distribution.\n")
	}

	// encode all requested volumes...
	if err = doEcEncode(commandEnv, writer, volumeIdToCollection, volumeIds, *maxParallelization, topologyInfo); err != nil {
		return fmt.Errorf("ec encode for volumes %v: %w", volumeIds, err)
	}
	// ...re-balance ec shards...
	if err := EcBalance(commandEnv, balanceCollections, "", rp, diskType, *maxParallelization, *applyBalancing); err != nil {
		return fmt.Errorf("re-balance ec shards for collection(s) %v: %w", balanceCollections, err)
	}
	// ...then delete original volumes using pre-collected locations.
	fmt.Printf("Deleting original volumes after EC encoding...\n")
	if err := doDeleteVolumesWithLocations(commandEnv, volumeIds, volumeLocationsMap, *maxParallelization); err != nil {
		return fmt.Errorf("delete original volumes after EC encoding: %w", err)
	}
	fmt.Printf("Successfully completed EC encoding for %d volumes\n", len(volumeIds))

	return nil
}

func volumeLocations(commandEnv *CommandEnv, volumeIds []needle.VolumeId) (map[needle.VolumeId][]wdclient.Location, error) {
	res := map[needle.VolumeId][]wdclient.Location{}
	for _, vid := range volumeIds {
		ls, ok := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
		if !ok {
			return nil, fmt.Errorf("volume %d not found", vid)
		}
		res[vid] = ls
	}

	return res, nil
}

func doEcEncode(commandEnv *CommandEnv, writer io.Writer, volumeIdToCollection map[needle.VolumeId]string, volumeIds []needle.VolumeId, maxParallelization int, topologyInfo *master_pb.TopologyInfo) error {
	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}
	locations, err := volumeLocations(commandEnv, volumeIds)
	if err != nil {
		return fmt.Errorf("failed to get volume locations for EC encoding: %w", err)
	}

	// build a map of (volumeId, serverAddress) -> freeVolumeCount
	freeVolumeCountMap := make(map[string]int) // key: volumeId-serverAddress
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				key := fmt.Sprintf("%d-%s", v.Id, dn.Id)
				freeVolumeCountMap[key] = int(diskInfo.FreeVolumeCount)
			}
		}
	})

	// mark volumes as readonly
	ewg := NewErrorWaitGroup(maxParallelization)
	for _, vid := range volumeIds {
		for _, l := range locations[vid] {
			ewg.Add(func() error {
				if err := markVolumeReplicaWritable(commandEnv.option.GrpcDialOption, vid, l, false, false); err != nil {
					return fmt.Errorf("mark volume %d as readonly on %s: %v", vid, l.Url, err)
				}
				return nil
			})
		}
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	// Sync replicas and select the best one for each volume (with highest file count)
	// This addresses data inconsistency risk in multi-replica volumes (issue #7797)
	// by syncing missing entries between replicas before encoding
	bestReplicas := make(map[needle.VolumeId]wdclient.Location)
	for _, vid := range volumeIds {
		locs := locations[vid]
		collection := volumeIdToCollection[vid]

		// Filter locations to only include those on healthy disks (FreeVolumeCount >= 2)
		var filteredLocs []wdclient.Location
		for _, l := range locs {
			key := fmt.Sprintf("%d-%s", vid, l.Url)
			if freeCount, found := freeVolumeCountMap[key]; found && freeCount >= 2 {
				filteredLocs = append(filteredLocs, l)
			}
		}

		if len(filteredLocs) == 0 {
			return fmt.Errorf("no healthy replicas (FreeVolumeCount >= 2) found for volume %d to use as source for EC encoding", vid)
		}

		// Sync missing entries between replicas, then select the best one
		bestLoc, selectErr := syncAndSelectBestReplica(commandEnv.option.GrpcDialOption, vid, collection, filteredLocs, "", writer)
		if selectErr != nil {
			return fmt.Errorf("failed to sync and select replica for volume %d: %v", vid, selectErr)
		}
		bestReplicas[vid] = bestLoc
	}

	// generate ec shards using the best replica for each volume
	ewg.Reset()
	for _, vid := range volumeIds {
		target := bestReplicas[vid]
		collection := volumeIdToCollection[vid]
		ewg.Add(func() error {
			if err := generateEcShards(commandEnv.option.GrpcDialOption, vid, collection, target.ServerAddress()); err != nil {
				return fmt.Errorf("generate ec shards for volume %d on %s: %v", vid, target.Url, err)
			}
			return nil
		})
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	// mount all ec shards for the converted volume
	shardIds := erasure_coding.AllShardIds()

	ewg.Reset()
	for _, vid := range volumeIds {
		target := bestReplicas[vid]
		collection := volumeIdToCollection[vid]
		ewg.Add(func() error {
			if err := mountEcShards(commandEnv.option.GrpcDialOption, collection, vid, target.ServerAddress(), shardIds); err != nil {
				return fmt.Errorf("mount ec shards for volume %d on %s: %v", vid, target.Url, err)
			}
			return nil
		})
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	return nil
}

// doDeleteVolumesWithLocations deletes volumes using pre-collected location information
// This avoids race conditions where master metadata is updated after EC encoding
func doDeleteVolumesWithLocations(commandEnv *CommandEnv, volumeIds []needle.VolumeId, volumeLocationsMap map[needle.VolumeId][]wdclient.Location, maxParallelization int) error {
	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	ewg := NewErrorWaitGroup(maxParallelization)
	for _, vid := range volumeIds {
		locations, found := volumeLocationsMap[vid]
		if !found {
			fmt.Printf("warning: no locations found for volume %d, skipping deletion\n", vid)
			continue
		}

		for _, l := range locations {
			ewg.Add(func() error {
				if err := deleteVolume(commandEnv.option.GrpcDialOption, vid, l.ServerAddress(), false); err != nil {
					return fmt.Errorf("deleteVolume %s volume %d: %v", l.Url, vid, err)
				}
				fmt.Printf("deleted volume %d from %s\n", vid, l.Url)
				return nil
			})
		}
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	return nil
}

func generateEcShards(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, collection string, sourceVolumeServer pb.ServerAddress) error {

	fmt.Printf("generateEcShards %d (collection %q) on %s ...\n", volumeId, collection, sourceVolumeServer)

	err := operation.WithVolumeServerClient(false, sourceVolumeServer, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, genErr := volumeServerClient.VolumeEcShardsGenerate(context.Background(), &volume_server_pb.VolumeEcShardsGenerateRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
		})
		return genErr
	})

	return err

}

func collectVolumeIdsForEcEncode(commandEnv *CommandEnv, collectionPattern string, sourceDiskType *types.DiskType, fullPercentage float64, quietPeriod time.Duration, verbose bool) (vids []needle.VolumeId, matchedCollections []string, err error) {
	// compile regex pattern for collection matching
	collectionRegex, err := compileCollectionPattern(collectionPattern)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid collection pattern '%s': %v", collectionPattern, err)
	}

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	quietSeconds := int64(quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	fmt.Printf("collect volumes with collection pattern '%s', quiet for: %d seconds and %.1f%% full\n", collectionPattern, quietSeconds, fullPercentage)

	// Statistics for verbose mode
	var (
		totalVolumes    int
		remoteVolumes   int
		wrongCollection int
		wrongDiskType   int
		tooRecent       int
		tooSmall        int
		noFreeDisk      int
	)

	vidMap := make(map[uint32]bool)
	collectionSet := make(map[string]bool)
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				totalVolumes++

				// ignore remote volumes
				if v.RemoteStorageName != "" && v.RemoteStorageKey != "" {
					remoteVolumes++
					if verbose {
						fmt.Printf("skip volume %d on %s: remote volume (storage: %s, key: %s)\n",
							v.Id, dn.Id, v.RemoteStorageName, v.RemoteStorageKey)
					}
					continue
				}

				// check collection against regex pattern
				if !collectionRegex.MatchString(v.Collection) {
					wrongCollection++
					if verbose {
						fmt.Printf("skip volume %d on %s: collection doesn't match pattern (pattern: %s, actual: %s)\n",
							v.Id, dn.Id, collectionPattern, v.Collection)
					}
					continue
				}

				// track matched collection
				collectionSet[v.Collection] = true

				// check disk type
				if sourceDiskType != nil && types.ToDiskType(v.DiskType) != *sourceDiskType {
					wrongDiskType++
					if verbose {
						fmt.Printf("skip volume %d on %s: wrong disk type (expected: %s, actual: %s)\n",
							v.Id, dn.Id, sourceDiskType.ReadableString(), types.ToDiskType(v.DiskType).ReadableString())
					}
					continue
				}

				// check quiet period
				if v.ModifiedAtSecond+quietSeconds >= nowUnixSeconds {
					tooRecent++
					if verbose {
						fmt.Printf("skip volume %d on %s: too recently modified (last modified: %d seconds ago, required: %d seconds)\n",
							v.Id, dn.Id, nowUnixSeconds-v.ModifiedAtSecond, quietSeconds)
					}
					continue
				}

				// check size
				sizeThreshold := fullPercentage / 100 * float64(volumeSizeLimitMb) * 1024 * 1024
				if float64(v.Size) <= sizeThreshold {
					tooSmall++
					if verbose {
						fmt.Printf("skip volume %d on %s: too small (size: %.1f MB, threshold: %.1f MB, %.1f%% full)\n",
							v.Id, dn.Id, float64(v.Size)/(1024*1024), sizeThreshold/(1024*1024),
							float64(v.Size)*100/(float64(volumeSizeLimitMb)*1024*1024))
					}
					continue
				}

				// check free disk space
				if diskInfo.FreeVolumeCount < 2 {
					glog.V(0).Infof("replica %s %d on %s has no free disk", v.Collection, v.Id, dn.Id)
					if verbose {
						fmt.Printf("skip replica of volume %d on %s: insufficient free disk space (free volumes: %d, required: 2)\n",
							v.Id, dn.Id, diskInfo.FreeVolumeCount)
					}
					if _, found := vidMap[v.Id]; !found {
						vidMap[v.Id] = false
					}
				} else {
					if verbose {
						fmt.Printf("selected volume %d on %s: size %.1f MB (%.1f%% full), last modified %d seconds ago, free volumes: %d\n",
							v.Id, dn.Id, float64(v.Size)/(1024*1024),
							float64(v.Size)*100/(float64(volumeSizeLimitMb)*1024*1024),
							nowUnixSeconds-v.ModifiedAtSecond, diskInfo.FreeVolumeCount)
					}
					vidMap[v.Id] = true
				}
			}
		}
	})

	for vid, good := range vidMap {
		if good {
			vids = append(vids, needle.VolumeId(vid))
		} else {
			noFreeDisk++
		}
	}

	// Convert collection set to slice
	for collection := range collectionSet {
		matchedCollections = append(matchedCollections, collection)
	}
	sort.Strings(matchedCollections)

	// Print summary statistics in verbose mode or when no volumes selected
	if verbose || len(vids) == 0 {
		fmt.Printf("\nVolume selection summary:\n")
		fmt.Printf("  Total volumes examined: %d\n", totalVolumes)
		fmt.Printf("  Selected for encoding: %d\n", len(vids))
		fmt.Printf("  Collections matched: %v\n", matchedCollections)

		if totalVolumes > 0 {
			fmt.Printf("\nReasons for exclusion:\n")
			if remoteVolumes > 0 {
				fmt.Printf("  Remote volumes: %d\n", remoteVolumes)
			}
			if wrongCollection > 0 {
				fmt.Printf("  Collection doesn't match pattern: %d\n", wrongCollection)
			}
			if wrongDiskType > 0 {
				fmt.Printf("  Wrong disk type: %d\n", wrongDiskType)
			}
			if tooRecent > 0 {
				fmt.Printf("  Too recently modified: %d\n", tooRecent)
			}
			if tooSmall > 0 {
				fmt.Printf("  Too small (< %.1f%% full): %d\n", fullPercentage, tooSmall)
			}
			if noFreeDisk > 0 {
				fmt.Printf("  Insufficient free disk space: %d\n", noFreeDisk)
			}
		}
		fmt.Println()
	}

	return
}
