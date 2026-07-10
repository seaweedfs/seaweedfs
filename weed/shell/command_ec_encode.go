package shell

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_replica"
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

	ec.encode [-collection=""] [-fullPercent=95 -quietFor=1h] [-batchSize=10] [-verbose] [-sourceDiskType=<disk_type>] [-diskType=<disk_type>]
	ec.encode [-volumeId=<volume_id>|-volumeIds=<volume_id>,...] [-batchSize=10] [-verbose] [-diskType=<disk_type>]

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
	  -batchSize: encode/rebalance/verify/delete this many volumes at a time (default 10; 0 = all in one batch)
	  -volumeIds: comma-separated volume IDs to encode

	Each batch is committed independently. If a later batch fails, earlier batches
	are already encoded and their original volumes deleted.

	Examples:
	  # Encode SSD volumes to SSD EC shards (same tier)
	  ec.encode -collection=mybucket -sourceDiskType=ssd -diskType=ssd

	  # Encode SSD volumes to HDD EC shards (tier migration to cheaper storage)
	  ec.encode -collection=mybucket -sourceDiskType=ssd -diskType=hdd

	  # Encode all volumes to SSD EC shards
	  ec.encode -collection=mybucket -diskType=ssd

	  # Encode selected volume IDs and delete originals after each batch
	  ec.encode -volumeIds=101,102,103 -batchSize=2

	Re-balancing algorithm:
	` + ecBalanceAlgorithmDescription
}

func (c *commandEcEncode) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcEncode) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	encodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := encodeCommand.Int("volumeId", 0, "the volume id")
	volumeIdsStr := encodeCommand.String("volumeIds", "", "comma-separated volume ids")
	collection := encodeCommand.String("collection", "", "collection name or regex pattern")
	fullPercentage := encodeCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := encodeCommand.Duration("quietFor", time.Hour, "select volumes without no writes for this period")
	maxParallelization := encodeCommand.Int("maxParallelization", DefaultMaxParallelization, "run up to X tasks in parallel, whenever possible")
	batchSize := encodeCommand.Int("batchSize", DefaultEcBatchSize, "encode/re-balance/verify/delete up to this many volumes at a time (0 = all in one batch)")
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
	if *volumeId != 0 || strings.TrimSpace(*volumeIdsStr) != "" {
		if *volumeId != 0 && strings.TrimSpace(*volumeIdsStr) != "" {
			return fmt.Errorf("-volumeId and -volumeIds are mutually exclusive")
		}
		if *volumeId != 0 {
			volumeIds = append(volumeIds, needle.VolumeId(*volumeId))
		} else {
			volumeIds, err = parseEcEncodeVolumeIds(*volumeIdsStr)
			if err != nil {
				return err
			}
		}
	} else {
		// apply to all volumes for the given collection pattern (regex)
		volumeIds, _, err = collectVolumeIdsForEcEncode(commandEnv, *collection, sourceDiskType, *fullPercentage, *quietPeriod, *verbose)
		if err != nil {
			return err
		}
	}
	if len(volumeIds) == 0 {
		fmt.Println("No volumes, nothing to do.")
		return nil
	}
	if *batchSize < 0 {
		return fmt.Errorf("-batchSize must be >= 0")
	}

	batches := chunkVolumeIds(volumeIds, *batchSize)
	if len(batches) > 1 {
		fmt.Printf("Processing %d volumes in %d batch(es), batchSize=%d\n", len(volumeIds), len(batches), *batchSize)
	}
	for i, batchVolumeIds := range batches {
		if len(batches) > 1 {
			fmt.Printf("Starting EC encoding batch %d/%d with %d volumes: %v\n", i+1, len(batches), len(batchVolumeIds), batchVolumeIds)
		}
		if err := processEcEncodeBatch(commandEnv, writer, batchVolumeIds, rp, diskType, *maxParallelization, *applyBalancing, *collection); err != nil {
			return fmt.Errorf("ec encode batch %d/%d for volumes %v: %w", i+1, len(batches), batchVolumeIds, err)
		}
	}
	if len(batches) > 1 {
		fmt.Printf("Successfully completed EC encoding for %d volumes in %d batch(es)\n", len(volumeIds), len(batches))
	}

	return nil
}

func parseEcEncodeVolumeIds(volumeIdsStr string) ([]needle.VolumeId, error) {
	var volumeIds []needle.VolumeId
	seen := make(map[needle.VolumeId]bool)
	for _, part := range strings.Split(volumeIdsStr, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		vidValue, err := strconv.ParseUint(part, 10, 32)
		if err != nil || vidValue == 0 {
			return nil, fmt.Errorf("invalid volume id %q in -volumeIds", part)
		}
		// ParseUint with bitSize 32 bounds the value; convert through uint32
		// (matching the rest of the codebase) so the narrowing is provably safe.
		vid := needle.VolumeId(uint32(vidValue))
		if seen[vid] {
			continue
		}
		seen[vid] = true
		volumeIds = append(volumeIds, vid)
	}
	if len(volumeIds) == 0 {
		return nil, fmt.Errorf("-volumeIds does not contain any valid volume id")
	}
	return volumeIds, nil
}

func chunkVolumeIds(volumeIds []needle.VolumeId, batchSize int) [][]needle.VolumeId {
	if batchSize <= 0 || len(volumeIds) == 0 {
		return [][]needle.VolumeId{volumeIds}
	}
	var batches [][]needle.VolumeId
	for start := 0; start < len(volumeIds); start += batchSize {
		end := start + batchSize
		if end > len(volumeIds) {
			end = len(volumeIds)
		}
		batches = append(batches, volumeIds[start:end])
	}
	return batches
}

func processEcEncodeBatch(commandEnv *CommandEnv, writer io.Writer, volumeIds []needle.VolumeId, rp *super_block.ReplicaPlacement, diskType types.DiskType, maxParallelization int, applyBalancing bool, collectionForMessage string) error {
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	// Refuse to encode a volume that is already EC (present only as shards):
	// an EC volume has no .dat, so re-encoding it would tear down its only
	// copy before failing. A regular volume (with a .dat) passes. This closes
	// the operator-rerun / script-retry path; a worker racing the snapshot is
	// handled by encode fencing, not here.
	if err := assertEncodableRegularVolumes(topologyInfo, volumeIds); err != nil {
		return err
	}

	volumeIdToCollection := collectVolumeIdToCollection(topologyInfo, volumeIds)
	balanceCollections := collectCollectionsForVolumeIds(topologyInfo, volumeIds)

	fmt.Printf("Collecting volume locations for %d volumes before EC encoding...\n", len(volumeIds))
	volumeLocationsMap, err := volumeLocations(commandEnv, volumeIds)
	if err != nil {
		return fmt.Errorf("failed to collect volume locations before EC encoding: %w", err)
	}

	if err := checkEcEncodeCapacity(topologyInfo, len(volumeIds), diskType, collectionForMessage); err != nil {
		return err
	}

	skippedNodes, err := doEcEncode(commandEnv, writer, volumeIdToCollection, volumeIds, maxParallelization, topologyInfo)
	if err != nil {
		return fmt.Errorf("ec encode for volumes %v: %w", volumeIds, err)
	}
	// Mounting the new shards notifies the master asynchronously, and EcBalance
	// plans from a fresh topology snapshot: one taken before the mounts land
	// shows no shards for these volumes, so the balance plans no moves and
	// silently leaves every shard on the generation host.
	if err := waitForEcShardsToRegister(commandEnv, volumeIds); err != nil {
		return fmt.Errorf("wait for ec shards to register with the master: %w", err)
	}
	// EcBalance works at collection scope. In batch mode this intentionally
	// rebalances each collection after every batch so source volumes can be
	// safely verified and deleted without waiting for all batches to finish.
	// skippedNodes are excluded so a recovered node's stale orphan is never
	// paired with a new-generation shard.
	if err := EcBalance(commandEnv, balanceCollections, "", rp, diskType, maxParallelization, applyBalancing, skippedNodes); err != nil {
		return fmt.Errorf("re-balance ec shards for collection(s) %v: %w", balanceCollections, err)
	}
	if err := verifyEcShardsBeforeDelete(commandEnv, volumeIds, diskType, applyBalancing); err != nil {
		return fmt.Errorf("verify EC shards before deleting originals: %w", err)
	}
	fmt.Printf("Deleting original volumes after EC encoding...\n")
	if err := doDeleteVolumesWithLocations(commandEnv, volumeIds, volumeLocationsMap, maxParallelization); err != nil {
		return fmt.Errorf("delete original volumes after EC encoding: %w", err)
	}
	fmt.Printf("Successfully completed EC encoding for %d volumes\n", len(volumeIds))
	return nil
}

func checkEcEncodeCapacity(topologyInfo *master_pb.TopologyInfo, volumeCount int, diskType types.DiskType, collectionForMessage string) error {
	// Pre-flight check: verify the target disk type has capacity for EC shards.
	// This prevents encoding shards only to fail during rebalance. Reuse the
	// caller's topology snapshot instead of issuing another VolumeList to the
	// master per batch.
	_, totalFreeEcSlots := collectEcVolumeServersByDc(topologyInfo, "", diskType)

	// Each volume needs TotalShardsCount (14) shards distributed.
	requiredSlots := volumeCount * erasure_coding.TotalShardsCount
	if totalFreeEcSlots < 1 {
		if diskType != types.HardDriveType {
			tryDiskTypeMessage := "Try passing -diskType=hdd, or omit -diskType to use the default (hdd)"
			if collectionForMessage != "" {
				tryDiskTypeMessage = fmt.Sprintf("Try:\n  ec.encode -collection=%s -diskType=hdd\nOr omit -diskType to use the default (hdd)", collectionForMessage)
			}
			return fmt.Errorf("no free ec shard slots on disk type '%s'. The target disk type has no capacity.\n"+
				"Your volumes are likely on a different disk type. %s", diskType, tryDiskTypeMessage)
		}
		return fmt.Errorf("no free ec shard slots. only %d left on disk type '%s'", totalFreeEcSlots, diskType)
	}

	if totalFreeEcSlots < requiredSlots {
		fmt.Printf("Warning: limited EC shard capacity. Need %d slots for %d volumes, but only %d slots available on disk type '%s'.\n",
			requiredSlots, volumeCount, totalFreeEcSlots, diskType)
		fmt.Printf("Rebalancing may not achieve optimal distribution.\n")
	}
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

func doEcEncode(commandEnv *CommandEnv, writer io.Writer, volumeIdToCollection map[needle.VolumeId]string, volumeIds []needle.VolumeId, maxParallelization int, topologyInfo *master_pb.TopologyInfo) (skippedNodes map[pb.ServerAddress]struct{}, err error) {
	if !commandEnv.isLocked() {
		return nil, fmt.Errorf("lock is lost")
	}
	locations, err := volumeLocations(commandEnv, volumeIds)
	if err != nil {
		return nil, fmt.Errorf("failed to get volume locations for EC encoding: %w", err)
	}

	// Clear EC shards left by a previous failed/partial encode so a retry
	// starts clean and never mixes two encode runs. A node skipped here as
	// unreachable is excluded from the later balance: it may still hold a stale
	// orphan that, paired with a new-generation shard from a balance copy, would
	// mix generations on that node.
	skippedNodes, err = clearPreexistingEcShards(commandEnv, topologyInfo, volumeIds, volumeIdToCollection, maxParallelization)
	if err != nil {
		return nil, fmt.Errorf("clear pre-existing ec shards before encoding: %w", err)
	}

	// Build a map of (volumeId, serverAddress) -> freeVolumeCount.
	// Key by dn.Address so it matches wdclient.Location.Url. In deployments
	// where dn.Id is a short name (e.g. Kubernetes StatefulSet pod name)
	// while dn.Address is a FQDN:port, keying by dn.Id would never match the
	// location Url during the health-check lookup below.
	freeVolumeCountMap := make(map[string]int) // key: volumeId-serverAddress
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		addr := dn.Address
		if addr == "" {
			addr = dn.Id // older nodes use ip:port as id
		}
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				key := fmt.Sprintf("%d-%s", v.Id, addr)
				freeVolumeCountMap[key] = int(diskInfo.FreeVolumeCount)
			}
		}
	})

	// Filter replicas by free capacity BEFORE marking volumes readonly so that
	// a failed health check does not strand volumes in readonly state.
	filteredLocations := make(map[needle.VolumeId][]wdclient.Location)
	for _, vid := range volumeIds {
		var filteredLocs []wdclient.Location
		for _, l := range locations[vid] {
			key := fmt.Sprintf("%d-%s", vid, l.Url)
			if freeCount, found := freeVolumeCountMap[key]; found && freeCount >= 2 {
				filteredLocs = append(filteredLocs, l)
			}
		}
		if len(filteredLocs) == 0 {
			return nil, fmt.Errorf("no healthy replicas (FreeVolumeCount >= 2) found for volume %d to use as source for EC encoding", vid)
		}
		filteredLocations[vid] = filteredLocs
	}

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
		return nil, err
	}

	// Sync replicas and select the best one for each volume (with highest file count)
	// This addresses data inconsistency risk in multi-replica volumes (issue #7797)
	// by syncing missing entries between replicas before encoding
	bestReplicas := make(map[needle.VolumeId]wdclient.Location)
	for _, vid := range volumeIds {
		collection := volumeIdToCollection[vid]

		// Sync missing entries between replicas, then select the best one
		bestLoc, selectErr := volume_replica.SyncAndSelectBestReplica(commandEnv.option.GrpcDialOption, vid, collection, filteredLocations[vid], "", writer)
		if selectErr != nil {
			return nil, fmt.Errorf("failed to sync and select replica for volume %d: %v", vid, selectErr)
		}
		bestReplicas[vid] = bestLoc
	}

	// Re-attempt the orphan sweep on the nodes skipped as unreachable, now that
	// any node that recovered during readonly-marking and replica sync answers
	// again. A node whose teardown now succeeds is clean (and the generation host
	// re-wipes its own disks regardless), so it leaves the skipped set and can be
	// a balance source/target — otherwise its shards would never distribute off
	// it. A node that is still down stays skipped and excluded, preserving the
	// leniency for a genuinely-down node; such a node also cannot be the
	// generation host below, since VolumeEcShardsGenerate would fail to read .dat.
	if err := resweepSkippedNodes(commandEnv, skippedNodes, volumeIds, volumeIdToCollection, maxParallelization); err != nil {
		return nil, err
	}

	// A selected generation host still in skippedNodes after the re-sweep was
	// transport-down when we tried to clean it, so its stale orphans were never
	// removed and EcBalance excludes it as both source and target. If it recovers
	// just in time for generation, all shards land on a node we can neither clean
	// nor balance off — a single point of failure that union-only verification
	// still accepts, after which the originals are deleted. Abort instead.
	for _, vid := range volumeIds {
		genHost := bestReplicas[vid].ServerAddress()
		if _, stillSkipped := skippedNodes[genHost]; stillSkipped {
			return nil, fmt.Errorf("generate ec shards for volume %d aborted: selected source %s is still skipped after the orphan re-sweep", vid, genHost)
		}
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
		return nil, err
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
		return nil, err
	}

	return skippedNodes, nil
}

// clearPreexistingEcShards removes EC shards and index files left over from a
// previous (failed or partial) encode of the given volume ids, on every node
// that still reports them, so a fresh encode regenerates from a clean slate.
// Scans all disk types. The normal .dat/.idx — the source of truth for this
// encode — is untouched; only orphaned EC artifacts are deleted.
//
// Returns the set of nodes skipped as unreachable. A skipped node may still hold
// an un-deleted orphan from a prior run; if it recovers it must be kept out of
// this encode's shard distribution, or the balance could install the new
// generation alongside the stale orphan and mix generations on one node.
func clearPreexistingEcShards(commandEnv *CommandEnv, topologyInfo *master_pb.TopologyInfo, volumeIds []needle.VolumeId, volumeIdToCollection map[needle.VolumeId]string, maxParallelization int) (skipped map[pb.ServerAddress]struct{}, err error) {
	wanted := make(map[uint32]bool, len(volumeIds))
	for _, vid := range volumeIds {
		wanted[uint32(vid)] = true
	}

	// Note which (node, vid) pairs the topology already reports EC shards for:
	// those are mounted leftovers and cleaning them is required (fatal on
	// error). Every other (node, vid) is swept best-effort to catch UNMOUNTED
	// orphans left by a failed copy — invisible to the heartbeat, so absent
	// here. A node that is down or holds nothing is a harmless no-op; a node
	// unreachable now also cannot receive this encode's new generation, so a
	// surviving orphan there keeps its old identity and the read guard rejects
	// it. Always delete the full shard-id range so a wider custom ratio's
	// leftovers are covered too.
	reportedKey := func(addr pb.ServerAddress, vid uint32) string {
		return string(addr) + "\x00" + strconv.Itoa(int(vid))
	}
	reported := make(map[string]struct{})
	var nodes []pb.ServerAddress
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		addr := pb.NewServerAddressFromDataNode(dn)
		nodes = append(nodes, addr)
		for _, diskInfo := range dn.DiskInfos {
			for _, ecInfo := range diskInfo.EcShardInfos {
				if wanted[ecInfo.Id] {
					reported[reportedKey(addr, ecInfo.Id)] = struct{}{}
				}
			}
		}
	})

	allShardIds := make([]erasure_coding.ShardId, erasure_coding.MaxShardCount)
	for i := range allShardIds {
		allShardIds[i] = erasure_coding.ShardId(i)
	}

	if len(reported) > 0 {
		fmt.Printf("clearing stale EC shards reported for %d (node,volume) pair(s) before regenerating...\n", len(reported))
	}
	// Nodes skipped as unreachable, accumulated across the concurrent sweep tasks.
	skipped = make(map[pb.ServerAddress]struct{})
	var skippedMu sync.Mutex
	ewg := NewErrorWaitGroup(maxParallelization)
	for _, addr := range nodes {
		for _, vid := range volumeIds {
			fatal := false
			if _, ok := reported[reportedKey(addr, uint32(vid))]; ok {
				fatal = true
			}
			collection := volumeIdToCollection[vid]
			ewg.Add(func() error {
				if err := unmountAndDeleteEcShardsQuiet(commandEnv.option.GrpcDialOption, collection, vid, addr, allShardIds); err != nil {
					// Surface a reachable node whose delete genuinely failed (its orphan would
					// be re-stamped by a later copy installing the new .vif). A missing
					// full_teardown ack from a reachable pre-upgrade node is fatal too: it may
					// still hold an orphan a later copy would re-stamp into the new generation.
					// Stay best-effort only for a node that is truly unreachable: codes.Unavailable
					// alone is ambiguous — a genuinely-down node and a reachable Rust volume
					// server in maintenance mode both return it (a Go server returns Unknown for
					// maintenance, already fatal above). Confirm with a non-maintenance-gated Ping
					// before skipping; skip only when the Ping itself transport-failed (nodeDown).
					// A reachable maintenance node (nodeUp) CAN receive this generation, and an
					// inconclusive Ping (nodeLivenessUnknown, e.g. a pre-Ping server returning
					// Unimplemented — which means the node is up) does not prove the node is down,
					// so both stay fatal rather than silently leaving a stale EC generation.
					if fatal || errors.Is(err, errFullTeardownNotAcked) || !isNodeUnreachable(err) ||
						classifyNodeLiveness(pingVolumeServer(commandEnv.option.GrpcDialOption, addr)) != nodeDown {
						return fmt.Errorf("clear stale ec shards for volume %d on %s: %w", vid, addr, err)
					}
					glog.V(1).Infof("orphan sweep: volume %d on %s skipped (unreachable): %v", vid, addr, err)
					skippedMu.Lock()
					skipped[addr] = struct{}{}
					skippedMu.Unlock()
				}
				return nil
			})
		}
	}
	if err := ewg.Wait(); err != nil {
		return nil, err
	}
	return skipped, nil
}

// resweepSkippedNodes re-attempts the orphan teardown on the nodes that the
// initial sweep skipped as unreachable, just before shard generation. A node
// that recovered in the meantime — and is therefore eligible to host this
// encode's generation — has its teardown retried; if it now fully succeeds it is
// removed from skipped so the rebalance can use it as a source and move its
// shards off, instead of stranding all shards on the single generation host and
// collapsing fault tolerance. A node still transport-down stays skipped (the
// same leniency the initial sweep grants), and a node that came back reachable
// but whose delete genuinely failed is fatal, exactly as in the initial sweep,
// so a stale generation is never silently left behind. Mutates skipped in place.
func resweepSkippedNodes(commandEnv *CommandEnv, skipped map[pb.ServerAddress]struct{}, volumeIds []needle.VolumeId, volumeIdToCollection map[needle.VolumeId]string, maxParallelization int) error {
	if len(skipped) == 0 {
		return nil
	}

	allShardIds := make([]erasure_coding.ShardId, erasure_coding.MaxShardCount)
	for i := range allShardIds {
		allShardIds[i] = erasure_coding.ShardId(i)
	}

	addrs := make([]pb.ServerAddress, 0, len(skipped))
	for addr := range skipped {
		addrs = append(addrs, addr)
	}

	fmt.Printf("re-checking %d node(s) skipped by the orphan sweep before generating shards...\n", len(addrs))

	// A node still down on every retried vid stays skipped; one that fully
	// succeeds is un-skipped. Track per-node whether any retry still failed
	// (down) so a node whose state is mixed across vids never gets un-skipped.
	stillDown := make(map[pb.ServerAddress]struct{})
	var mu sync.Mutex
	ewg := NewErrorWaitGroup(maxParallelization)
	for _, addr := range addrs {
		for _, vid := range volumeIds {
			collection := volumeIdToCollection[vid]
			ewg.Add(func() error {
				if err := unmountAndDeleteEcShardsQuiet(commandEnv.option.GrpcDialOption, collection, vid, addr, allShardIds); err != nil {
					// Same decision as the initial sweep: a reachable node whose delete
					// genuinely failed (or did not ack a full teardown, or whose liveness is
					// inconclusive) is fatal, since it could hold an orphan a later copy
					// re-stamps into this generation. Only a node still transport-down stays
					// skipped.
					if errors.Is(err, errFullTeardownNotAcked) || !isNodeUnreachable(err) ||
						classifyNodeLiveness(pingVolumeServer(commandEnv.option.GrpcDialOption, addr)) != nodeDown {
						return fmt.Errorf("re-clear stale ec shards for volume %d on %s: %w", vid, addr, err)
					}
					glog.V(1).Infof("orphan re-sweep: volume %d on %s still skipped (unreachable): %v", vid, addr, err)
					mu.Lock()
					stillDown[addr] = struct{}{}
					mu.Unlock()
				}
				return nil
			})
		}
	}
	if err := ewg.Wait(); err != nil {
		return err
	}

	for _, addr := range addrs {
		if _, down := stillDown[addr]; !down {
			delete(skipped, addr)
			glog.V(0).Infof("orphan re-sweep: node %s recovered and was cleaned; it will participate in the EC rebalance", addr)
		}
	}
	return nil
}

// isNodeUnreachable reports whether err means the volume server could not be
// reached at all, as opposed to an RPC that reached the node and failed. Only an
// unreachable node is safe to skip in the orphan sweep. A dead peer surfaces as
// a gRPC codes.Unavailable from the RPC (the dial is lazy, so it never fails at
// connect time); any non-status error reached node logic and is treated as
// reachable, so the sweep stays fatal rather than silently leaving stale state.
func isNodeUnreachable(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
}

// nodeLiveness is the tri-state result of a pingVolumeServer probe.
type nodeLiveness int

const (
	// nodeUp: Ping succeeded — the node is reachable (e.g. a Rust volume server
	// in maintenance mode that fails the delete but answers Ping).
	nodeUp nodeLiveness = iota
	// nodeDown: Ping itself transport-failed with codes.Unavailable — the node is
	// confirmed unreachable. The only state the orphan sweep may skip.
	nodeDown
	// nodeLivenessUnknown: Ping reached failing logic with any non-Unavailable
	// code (Internal, ResourceExhausted, Unimplemented from a pre-Ping server, …)
	// or a non-status error. This does NOT prove the node is down, so it is fatal.
	nodeLivenessUnknown
)

// classifyNodeLiveness maps a pingVolumeServer error into the tri-state. A nil
// error is nodeUp, a transport codes.Unavailable is nodeDown (reusing the same
// rule as isNodeUnreachable), and every other Ping failure is nodeLivenessUnknown.
func classifyNodeLiveness(pingErr error) nodeLiveness {
	if pingErr == nil {
		return nodeUp
	}
	if isNodeUnreachable(pingErr) {
		return nodeDown
	}
	return nodeLivenessUnknown
}

// collectEcShardBitsByNode returns, for one volume, the EC shard bits each node
// reports, unioned across all its disk types. Freshly generated shards sit on
// the disk that held the source .dat, which may differ from the balance target
// disk type, so visibility questions ("has the master heard about these shards
// at all?") must not filter by disk type. Only the newest encode generation
// (largest EncodeTsNs) counts: an orphaned older generation — a failed earlier
// encode on a node the pre-encode sweep could not reach but the master still
// hears from — must neither satisfy the registration wait nor pose as a second
// holder in the clump check. Entries without a timestamp form the legacy
// generation zero, so they only count when no stamped generation exists;
// dropping them otherwise errs toward keeping the source volume.
func collectEcShardBitsByNode(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId) map[pb.ServerAddress]erasure_coding.ShardBits {
	type shardEntry struct {
		addr pb.ServerAddress
		ts   int64
		bits erasure_coding.ShardBits
	}
	var entries []shardEntry
	var newestTs int64
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, ecInfo := range diskInfo.EcShardInfos {
				if ecInfo.Id != uint32(vid) {
					continue
				}
				entries = append(entries, shardEntry{pb.NewServerAddressFromDataNode(dn), ecInfo.EncodeTsNs, erasure_coding.ShardBits(ecInfo.EcIndexBits)})
				if ecInfo.EncodeTsNs > newestTs {
					newestTs = ecInfo.EncodeTsNs
				}
			}
		}
	})
	res := make(map[pb.ServerAddress]erasure_coding.ShardBits)
	for _, e := range entries {
		if e.ts == newestTs {
			res[e.addr] |= e.bits
		}
	}
	return res
}

// waitForEcShardsToRegister polls the master topology until every given volume
// reports a full EC shard set. Mounting shards notifies the master
// asynchronously (mount -> NewEcShardsChan -> delta heartbeat), so a topology
// snapshot taken right after doEcEncode can predate the mounts. Failing after
// the retries keeps the source volumes, and a re-run of ec.encode starts clean.
func waitForEcShardsToRegister(commandEnv *CommandEnv, volumeIds []needle.VolumeId) error {
	const maxAttempts = 10
	const retryInterval = 2 * time.Second

	var lastMissing []string
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(retryInterval)
		}
		topoInfo, _, err := collectTopologyInfo(commandEnv, 0)
		if err != nil {
			return fmt.Errorf("fetch topology while waiting for ec shards to register: %w", err)
		}
		lastMissing = lastMissing[:0]
		for _, vid := range volumeIds {
			var union erasure_coding.ShardBits
			for _, bits := range collectEcShardBitsByNode(topoInfo, vid) {
				union |= bits
			}
			if union.Count() < erasure_coding.TotalShardsCount {
				lastMissing = append(lastMissing, fmt.Sprintf("volume %d: %d/%d shards", vid, union.Count(), erasure_coding.TotalShardsCount))
			}
		}
		if len(lastMissing) == 0 {
			return nil
		}
		glog.V(0).Infof("waiting for newly generated ec shards to register with the master (attempt %d/%d): %v",
			attempt+1, maxAttempts, lastMissing)
	}
	return fmt.Errorf("newly generated ec shards did not register with the master after %d attempts: %v", maxAttempts, lastMissing)
}

// ecShardsClumpedOnOneNode reports whether every EC shard the master sees for
// vid sits on a single node while at least one other node has free EC shard
// slots on the target disk type — i.e. the preceding rebalance could have
// spread the shards but did not. Zero visible shards is not a clump; the
// recoverability check owns that case.
func ecShardsClumpedOnOneNode(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId, diskType types.DiskType) (holder pb.ServerAddress, clumped bool) {
	byNode := collectEcShardBitsByNode(topoInfo, vid)
	if len(byNode) != 1 {
		return "", false
	}
	for addr := range byNode {
		holder = addr
	}
	ecNodes, _ := collectEcVolumeServersByDc(topoInfo, "", diskType)
	for _, en := range ecNodes {
		if pb.NewServerAddressFromDataNode(en.info) == holder {
			continue
		}
		if en.freeEcSlot > 0 {
			return holder, true
		}
	}
	return "", false
}

func verifyEcShardsBeforeDelete(commandEnv *CommandEnv, volumeIds []needle.VolumeId, diskType types.DiskType, expectSpread bool) error {
	// Shard relocations from the preceding EC balance reach the master via
	// volume-server heartbeats, so freshly distributed shards may not all be
	// visible in the master topology immediately. Poll a few times before
	// concluding the shard set is incomplete, so a heartbeat-propagation lag is
	// not mistaken for missing data. After the retries: a volume below the
	// recoverable threshold (dataShards) aborts the deletion; a recoverable
	// but degraded set proceeds with a warning, since the missing shards can
	// be rebuilt from the survivors while keeping the source next to live
	// shards is the more dangerous mixed state. When expectSpread is set (the
	// rebalance ran in apply mode), a volume whose shards all still sit on one
	// node while another node has free slots also aborts the deletion: losing
	// that node would lose the volume, so the original is the safer copy.
	const maxAttempts = 10
	const retryInterval = 2 * time.Second

	var lastErr error
	var lastDegraded []string
	var lastClumped []string
	for attempt := 0; attempt < maxAttempts; attempt++ {
		topoInfo, _, err := collectTopologyInfo(commandEnv, 0)
		if err != nil {
			return fmt.Errorf("fetch topology for shard verification: %w", err)
		}

		lastErr = nil
		lastDegraded = lastDegraded[:0]
		lastClumped = lastClumped[:0]
		for _, vid := range volumeIds {
			nodeShards, _ := collectEcNodeShardsInfo(topoInfo, vid, diskType)

			var union erasure_coding.ShardBits
			for _, info := range nodeShards {
				union = erasure_coding.ShardBits(uint32(union) | info.Bitmap())
			}

			totalShards := erasure_coding.TotalShardsCount
			degraded, err := erasure_coding.RequireRecoverableShardSet(uint32(vid), union, erasure_coding.DataShardsCount, totalShards)
			if err != nil {
				summary := make([]string, 0, len(nodeShards))
				for node, info := range nodeShards {
					summary = append(summary, fmt.Sprintf("%s=%s", node, info.String()))
				}
				sort.Strings(summary)
				lastErr = fmt.Errorf("volume %d: %w (observed: %v)", vid, err, summary)
				break
			}
			if expectSpread {
				if holder, clumped := ecShardsClumpedOnOneNode(topoInfo, vid, diskType); clumped {
					lastClumped = append(lastClumped, fmt.Sprintf("volume %d: all shards on %s", vid, holder))
					continue
				}
			}
			if degraded {
				lastDegraded = append(lastDegraded, fmt.Sprintf("volume %d: %d/%d shards", vid, union.Count(), totalShards))
				continue
			}

			glog.V(0).Infof("EC shard verification ok for volume %d on diskType %q: %d/%d shards present across %d nodes",
				vid, diskType.ReadableString(), union.Count(), totalShards, len(nodeShards))
		}

		if lastErr == nil && len(lastDegraded) == 0 && len(lastClumped) == 0 {
			return nil
		}
		if attempt < maxAttempts-1 {
			glog.V(0).Infof("EC shard verification incomplete (attempt %d/%d), waiting for shard locations to propagate: %v %v %v",
				attempt+1, maxAttempts, lastErr, lastDegraded, lastClumped)
			time.Sleep(retryInterval)
		}
	}

	if lastErr != nil {
		glog.Errorf("EC shard verification failed after %d attempts: %v", maxAttempts, lastErr)
		return lastErr
	}
	if len(lastClumped) > 0 {
		return fmt.Errorf("EC shards still sit on a single node after rebalance even though other nodes have free slots (%v); keeping the original volumes. Run ec.balance -apply, verify the spread, then delete the originals or re-run ec.encode", lastClumped)
	}
	glog.Warningf("EC shard set incomplete but recoverable after %d attempts, proceeding with source deletion (rebuild missing shards with ec.rebuild): %v",
		maxAttempts, lastDegraded)
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
				if err := deleteVolume(commandEnv.option.GrpcDialOption, vid, l.ServerAddress(), false, false); err != nil {
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

	vids, matchedCollections = selectVolumeIdsFromTopology(topologyInfo, volumeSizeLimitMb, collectionRegex, sourceDiskType, quietSeconds, nowUnixSeconds, fullPercentage, verbose)
	return
}

func selectVolumeIdsFromTopology(topologyInfo *master_pb.TopologyInfo, volumeSizeLimitMb uint64, collectionRegex *regexp.Regexp, sourceDiskType *types.DiskType, quietSeconds int64, nowUnixSeconds int64, fullPercentage float64, verbose bool) (vids []needle.VolumeId, matchedCollections []string) {
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
							v.Id, dn.Id, collectionRegex.String(), v.Collection)
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
