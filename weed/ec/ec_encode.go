package ec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/operation/volume_move"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_replica"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
)

// markVolumeReplicaWritable marks one replica writable/readonly with a progress
// line, delegating to the canonical volume_move helper.
func markVolumeReplicaWritable(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, location wdclient.Location, writable, persist bool) error {
	if writable {
		fmt.Printf("markVolumeWritable %d on %s ...\n", volumeId, location.Url)
	} else {
		fmt.Printf("markVolumeReadonly %d on %s persist=%v ...\n", volumeId, location.Url, persist)
	}
	return volume_move.NewMover(grpcDialOption).MarkVolumeWritable(ctx, volumeId, location.ServerAddress(), writable, persist)
}

// deleteVolume removes the volume from sourceVolumeServer via the canonical
// volume_move helper.
func deleteVolume(ctx context.Context, grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceVolumeServer pb.ServerAddress, onlyEmpty bool, keepRemoteData bool) (err error) {
	return volume_move.NewMover(grpcDialOption).DeleteVolume(ctx, volumeId, sourceVolumeServer, onlyEmpty, keepRemoteData)
}

func ChunkVolumeIds(volumeIds []needle.VolumeId, batchSize int) [][]needle.VolumeId {
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

func ProcessEcEncodeBatch(env *Env, writer io.Writer, volumeIds []needle.VolumeId, rp *super_block.ReplicaPlacement, diskType types.DiskType, maxParallelization int, applyBalancing bool, collectionForMessage string) (err error) {
	topologyInfo, _, err := env.FetchTopology(0)
	if err != nil {
		return err
	}

	// Refuse to encode a volume that is already EC (present only as shards):
	// an EC volume has no .dat, so re-encoding it would tear down its only
	// copy before failing. A regular volume (with a .dat) passes. This closes
	// the operator-rerun / script-retry path; a worker racing the snapshot is
	// handled by encode fencing, not here.
	if err := AssertEncodableRegularVolumes(topologyInfo, volumeIds); err != nil {
		return err
	}

	volumeIdToCollection := CollectVolumeIdToCollection(topologyInfo, volumeIds)
	balanceCollections := CollectCollectionsForVolumeIds(topologyInfo, volumeIds)

	fmt.Printf("Collecting volume locations for %d volumes before EC encoding...\n", len(volumeIds))
	volumeLocationsMap, err := volumeLocations(env, volumeIds)
	if err != nil {
		return fmt.Errorf("failed to collect volume locations before EC encoding: %w", err)
	}

	if err := checkEcEncodeCapacity(topologyInfo, len(volumeIds), diskType, collectionForMessage); err != nil {
		return err
	}

	// From here doEcEncode marks the volumes readonly and generates EC shards.
	// If any step before the originals are deleted fails, roll the encode back:
	// tear down the shards produced this run and restore the sources to writable,
	// so a failed (and possibly abandoned) ec.encode does not strand volumes
	// readonly or leave orphan EC shards behind. Once the shards are verified
	// recoverable we are committed to the EC copy and must not roll back.
	committed := false
	defer func() {
		if err != nil && !committed {
			rollbackFailedEcEncode(env, writer, volumeIds, volumeIdToCollection, volumeLocationsMap, maxParallelization)
		}
	}()

	skippedNodes, err := doEcEncode(env, writer, volumeIdToCollection, volumeIds, maxParallelization, topologyInfo)
	if err != nil {
		return fmt.Errorf("ec encode for volumes %v: %w", volumeIds, err)
	}
	// Mounting the new shards notifies the master asynchronously, and EcBalance
	// plans from a fresh topology snapshot: one taken before the mounts land
	// shows no shards for these volumes, so the balance plans no moves and
	// silently leaves every shard on the generation host.
	if err := waitForEcShardsToRegister(env, volumeIds); err != nil {
		return fmt.Errorf("wait for ec shards to register with the master: %w", err)
	}
	// EcBalance works at collection scope. In batch mode this intentionally
	// rebalances each collection after every batch so source volumes can be
	// safely verified and deleted without waiting for all batches to finish.
	// skippedNodes are excluded so a recovered node's stale orphan is never
	// paired with a new-generation shard.
	if err := EcBalance(env, balanceCollections, "", rp, diskType, maxParallelization, 0, applyBalancing, skippedNodes, nil, volumeIds); err != nil {
		return fmt.Errorf("re-balance ec shards for collection(s) %v: %w", balanceCollections, err)
	}
	if err := verifyEcShardsBeforeDelete(env, volumeIds, diskType, applyBalancing); err != nil {
		return fmt.Errorf("verify EC shards before deleting originals: %w", err)
	}
	// Past verify the EC copy is recoverable; a delete failure below must not
	// tear the shards down.
	committed = true
	fmt.Printf("Deleting original volumes after EC encoding...\n")
	if err := doDeleteVolumesWithLocations(env, volumeIds, volumeLocationsMap, maxParallelization); err != nil {
		return fmt.Errorf("delete original volumes after EC encoding: %w", err)
	}
	fmt.Printf("Successfully completed EC encoding for %d volumes\n", len(volumeIds))
	return nil
}

// rollbackFailedEcEncode is a best-effort cleanup for an ec.encode that failed
// after marking volumes readonly / generating shards but before the originals
// were deleted. It tears down the EC shards this run produced (so they do not
// survive as orphans until the next encode) and restores the source volumes to
// writable (so a failed and possibly abandoned encode does not strand them
// readonly). Errors are logged, not returned — we are already on the failure
// path. Both operations are idempotent: clearPreexistingEcShards is a no-op when
// no shards were generated, and marking an already-writable volume is a no-op,
// so it is safe even for a failure before the volumes were marked readonly.
func rollbackFailedEcEncode(env *Env, writer io.Writer, volumeIds []needle.VolumeId, volumeIdToCollection map[needle.VolumeId]string, volumeLocationsMap map[needle.VolumeId][]wdclient.Location, maxParallelization int) {
	fmt.Fprintf(writer, "rolling back failed EC encode for volumes %v...\n", volumeIds)

	// Tear down any EC shards this run produced. A fresh topology snapshot finds
	// them wherever generate/balance left them; the teardown is blanket.
	if topologyInfo, _, err := env.FetchTopology(0); err != nil {
		fmt.Fprintf(writer, "rollback: collect topology to clear ec shards: %v\n", err)
	} else if _, err := clearPreexistingEcShards(env, topologyInfo, volumeIds, volumeIdToCollection, maxParallelization); err != nil {
		fmt.Fprintf(writer, "rollback: clear ec shards: %v\n", err)
	}

	// Restore the source volumes to writable. doEcEncode re-reads the locations
	// and marks every replica of that later snapshot readonly, so re-read here
	// too: a replica added or moved between the batch's initial snapshot
	// (volumeLocationsMap) and doEcEncode's readonly-marking would otherwise be
	// left readonly. Fall back to the initial snapshot if the re-read fails.
	locations := volumeLocationsMap
	if fresh, err := volumeLocations(env, volumeIds); err != nil {
		fmt.Fprintf(writer, "rollback: re-read volume locations (using pre-encode snapshot): %v\n", err)
	} else {
		locations = fresh
	}
	ewg := util.NewErrorWaitGroup(maxParallelization)
	for _, vid := range volumeIds {
		for _, l := range locations[vid] {
			ewg.Add(func() error {
				if err := markVolumeReplicaWritable(context.Background(), env.GrpcDialOption, vid, l, true, false); err != nil {
					return fmt.Errorf("restore volume %d writable on %s: %w", vid, l.Url, err)
				}
				return nil
			})
		}
	}
	if err := ewg.Wait(); err != nil {
		fmt.Fprintf(writer, "rollback: %v\n", err)
	}
}

func checkEcEncodeCapacity(topologyInfo *master_pb.TopologyInfo, volumeCount int, diskType types.DiskType, collectionForMessage string) error {
	// Pre-flight check: verify the target disk type has capacity for EC shards.
	// This prevents encoding shards only to fail during rebalance. Reuse the
	// caller's topology snapshot instead of issuing another VolumeList to the
	// master per batch.
	_, totalFreeEcSlots := CollectEcVolumeServersByDc(topologyInfo, "", diskType)

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

func volumeLocations(env *Env, volumeIds []needle.VolumeId) (map[needle.VolumeId][]wdclient.Location, error) {
	res := map[needle.VolumeId][]wdclient.Location{}
	for _, vid := range volumeIds {
		ls, ok := env.GetVolumeLocations(uint32(vid))
		if !ok {
			return nil, fmt.Errorf("volume %d not found", vid)
		}
		res[vid] = ls
	}

	return res, nil
}

// collectSourceFreeVolumeCounts maps "volumeId-serverAddress" to the free
// volume slots on the disk holding that volume, for the source health check.
// Key by dn.Address so it matches wdclient.Location.Url: in deployments where
// dn.Id is a short name (e.g. a Kubernetes StatefulSet pod name) while
// dn.Address is a FQDN:port, keying by dn.Id would never match the location
// Url during the lookup.
//
// The topology snapshot predates clearPreexistingEcShards, so shards of the
// very volumes being encoded (leftovers of an interrupted run the sweep just
// removed) may still be charged against a disk's FreeVolumeCount. Refund their
// slots, or a retried encode fails the health check on capacity the sweep
// already freed. A node the sweep skipped as unreachable keeps its charge: its
// leftovers are still there.
func collectSourceFreeVolumeCounts(topologyInfo *master_pb.TopologyInfo, volumeIds []needle.VolumeId, sweepSkippedNodes map[pb.ServerAddress]struct{}) map[string]int {
	encoding := make(map[uint32]bool, len(volumeIds))
	for _, vid := range volumeIds {
		encoding[uint32(vid)] = true
	}
	freeVolumeCountMap := make(map[string]int) // key: volumeId-serverAddress
	EachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		addr := dn.Address
		if addr == "" {
			addr = dn.Id // older nodes use ip:port as id
		}
		_, skipped := sweepSkippedNodes[pb.NewServerAddressFromDataNode(dn)]
		for _, diskInfo := range dn.DiskInfos {
			free := diskInfo.FreeVolumeCount
			if !skipped {
				var total, cleared int64
				for _, ecInfo := range diskInfo.EcShardInfos {
					n := int64(erasure_coding.GetShardCount(ecInfo))
					total += n
					if encoding[ecInfo.Id] {
						cleared += n
					}
				}
				if cleared > 0 {
					free += erasure_coding.VolumeSlots(total) - erasure_coding.VolumeSlots(total-cleared)
				}
			}
			for _, v := range diskInfo.VolumeInfos {
				key := fmt.Sprintf("%d-%s", v.Id, addr)
				freeVolumeCountMap[key] = int(free)
			}
		}
	})
	return freeVolumeCountMap
}

func doEcEncode(env *Env, writer io.Writer, volumeIdToCollection map[needle.VolumeId]string, volumeIds []needle.VolumeId, maxParallelization int, topologyInfo *master_pb.TopologyInfo) (skippedNodes map[pb.ServerAddress]struct{}, err error) {
	if !env.isLocked() {
		return nil, fmt.Errorf("lock is lost")
	}
	locations, err := volumeLocations(env, volumeIds)
	if err != nil {
		return nil, fmt.Errorf("failed to get volume locations for EC encoding: %w", err)
	}

	// Clear EC shards left by a previous failed/partial encode so a retry
	// starts clean and never mixes two encode runs. A node skipped here as
	// unreachable is excluded from the later balance: it may still hold a stale
	// orphan that, paired with a new-generation shard from a balance copy, would
	// mix generations on that node.
	skippedNodes, err = clearPreexistingEcShards(env, topologyInfo, volumeIds, volumeIdToCollection, maxParallelization)
	if err != nil {
		return nil, fmt.Errorf("clear pre-existing ec shards before encoding: %w", err)
	}

	freeVolumeCountMap := collectSourceFreeVolumeCounts(topologyInfo, volumeIds, skippedNodes)

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
	ewg := util.NewErrorWaitGroup(maxParallelization)
	for _, vid := range volumeIds {
		for _, l := range locations[vid] {
			ewg.Add(func() error {
				if err := markVolumeReplicaWritable(context.Background(), env.GrpcDialOption, vid, l, false, false); err != nil {
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
		bestLoc, selectErr := volume_replica.SyncAndSelectBestReplica(env.GrpcDialOption, vid, collection, filteredLocations[vid], "", writer)
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
	if err := resweepSkippedNodes(env, skippedNodes, volumeIds, volumeIdToCollection, maxParallelization); err != nil {
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
			if err := generateEcShards(env.GrpcDialOption, vid, collection, target.ServerAddress()); err != nil {
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
			if err := MountEcShards(env.GrpcDialOption, collection, vid, target.ServerAddress(), shardIds); err != nil {
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
func clearPreexistingEcShards(env *Env, topologyInfo *master_pb.TopologyInfo, volumeIds []needle.VolumeId, volumeIdToCollection map[needle.VolumeId]string, maxParallelization int) (skipped map[pb.ServerAddress]struct{}, err error) {
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
		return string(addr) + "\x00" + strconv.FormatUint(uint64(vid), 10)
	}
	reported := make(map[string]struct{})
	var nodes []pb.ServerAddress
	EachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
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
	ewg := util.NewErrorWaitGroup(maxParallelization)
	for _, addr := range nodes {
		for _, vid := range volumeIds {
			fatal := false
			if _, ok := reported[reportedKey(addr, uint32(vid))]; ok {
				fatal = true
			}
			collection := volumeIdToCollection[vid]
			ewg.Add(func() error {
				if err := UnmountAndDeleteEcShardsQuiet(env.GrpcDialOption, collection, vid, addr, allShardIds); err != nil {
					// Surface a reachable node whose delete genuinely failed (its orphan would
					// be re-stamped by a later copy installing the new .vif). A missing
					// full_teardown ack from a reachable pre-upgrade node is fatal too: it may
					// still hold an orphan a later copy would re-stamp into the new generation.
					// Stay best-effort only for a node that is truly unreachable: codes.Unavailable
					// alone is ambiguous — a genuinely-down node and a reachable Rust volume
					// server in maintenance mode both return it (a Go server returns Unknown for
					// maintenance, already fatal above). Confirm with a non-maintenance-gated Ping
					// before skipping; skip only when the Ping itself transport-failed (NodeDown).
					// A reachable maintenance node (nodeUp) CAN receive this generation, and an
					// inconclusive Ping (nodeLivenessUnknown, e.g. a pre-Ping server returning
					// Unimplemented — which means the node is up) does not prove the node is down,
					// so both stay fatal rather than silently leaving a stale EC generation.
					if fatal || errors.Is(err, ErrFullTeardownNotAcked) || !IsNodeUnreachable(err) ||
						ClassifyNodeLiveness(PingVolumeServer(env.GrpcDialOption, addr)) != NodeDown {
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
func resweepSkippedNodes(env *Env, skipped map[pb.ServerAddress]struct{}, volumeIds []needle.VolumeId, volumeIdToCollection map[needle.VolumeId]string, maxParallelization int) error {
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
	ewg := util.NewErrorWaitGroup(maxParallelization)
	for _, addr := range addrs {
		for _, vid := range volumeIds {
			collection := volumeIdToCollection[vid]
			ewg.Add(func() error {
				if err := UnmountAndDeleteEcShardsQuiet(env.GrpcDialOption, collection, vid, addr, allShardIds); err != nil {
					// Same decision as the initial sweep: a reachable node whose delete
					// genuinely failed (or did not ack a full teardown, or whose liveness is
					// inconclusive) is fatal, since it could hold an orphan a later copy
					// re-stamps into this generation. Only a node still transport-down stays
					// skipped.
					if errors.Is(err, ErrFullTeardownNotAcked) || !IsNodeUnreachable(err) ||
						ClassifyNodeLiveness(PingVolumeServer(env.GrpcDialOption, addr)) != NodeDown {
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
func CollectEcShardBitsByNode(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId) map[pb.ServerAddress]erasure_coding.ShardBits {
	type shardEntry struct {
		addr pb.ServerAddress
		ts   int64
		bits erasure_coding.ShardBits
	}
	var entries []shardEntry
	var newestTs int64
	EachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
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

// collectNewestGenerationShardsInfo answers the same question as
// CollectEcShardBitsByNode -- which shards belong to the newest encode
// generation -- and keeps the sizes with them.
//
// The two have to agree on the generation. A re-encode can change the ratio,
// so an orphaned older generation's shards are a different length by nature:
// merging them into the size comparison makes a healthy current set look
// inconsistent, and since the orphan keeps being reported, every retry fails
// and the encode is left holding both the volume and its shards forever.
func collectNewestGenerationShardsInfo(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId) map[pb.ServerAddress]*erasure_coding.ShardsInfo {
	type shardEntry struct {
		addr pb.ServerAddress
		ts   int64
		info *erasure_coding.ShardsInfo
	}
	var entries []shardEntry
	var newestTs int64
	EachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			if diskInfo == nil {
				continue
			}
			for _, ecInfo := range diskInfo.EcShardInfos {
				if ecInfo.Id != uint32(vid) {
					continue
				}
				entries = append(entries, shardEntry{
					addr: pb.NewServerAddressFromDataNode(dn),
					ts:   ecInfo.EncodeTsNs,
					info: erasure_coding.ShardsInfoFromVolumeEcShardInformationMessage(ecInfo),
				})
				if ecInfo.EncodeTsNs > newestTs {
					newestTs = ecInfo.EncodeTsNs
				}
			}
		}
	})

	res := make(map[pb.ServerAddress]*erasure_coding.ShardsInfo)
	for _, e := range entries {
		if e.ts != newestTs {
			continue
		}
		if existing, ok := res[e.addr]; ok {
			existing.Add(e.info)
		} else {
			res[e.addr] = e.info
		}
	}
	return res
}

// waitForEcShardsToRegister polls the master topology until every given volume
// reports a full EC shard set. Mounting shards notifies the master
// asynchronously (mount -> NewEcShardsChan -> delta heartbeat), so a topology
// snapshot taken right after doEcEncode can predate the mounts. Failing after
// the retries keeps the source volumes, and a re-run of ec.encode starts clean.
func waitForEcShardsToRegister(env *Env, volumeIds []needle.VolumeId) error {
	const maxAttempts = 10
	const retryInterval = 2 * time.Second

	var lastMissing []string
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(retryInterval)
		}
		topoInfo, _, err := env.FetchTopology(0)
		if err != nil {
			return fmt.Errorf("fetch topology while waiting for ec shards to register: %w", err)
		}
		lastMissing = lastMissing[:0]
		for _, vid := range volumeIds {
			var union erasure_coding.ShardBits
			for _, bits := range CollectEcShardBitsByNode(topoInfo, vid) {
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
	byNode := CollectEcShardBitsByNode(topoInfo, vid)
	if len(byNode) != 1 {
		return "", false
	}
	for addr := range byNode {
		holder = addr
	}
	ecNodes, _ := CollectEcVolumeServersByDc(topoInfo, "", diskType)
	for _, en := range ecNodes {
		if pb.NewServerAddressFromDataNode(en.Info) == holder {
			continue
		}
		if en.FreeEcSlot > 0 {
			return holder, true
		}
	}
	return "", false
}

// ecShardSummaryByNode says where a volume's shards are, one entry per node,
// sorted so the message is stable. It names the ids and not just the count: a
// set holding shards 0-9 and one holding 4-13 are both "10 shards", and which
// ones survived is what says whether the set is recoverable and from where.
// requireUniformShardSizes reports shards whose size disagrees with the rest.
// Every shard of a volume takes one piece of each block row, so they are all
// written to the same length -- an odd one out is a shard that was truncated,
// half copied, or written to a disk that filled up. Counting shards cannot see
// that, and the encode is about to delete the volume they were made from.
//
// Sizes the cluster does not report (zero) are skipped rather than treated as
// a disagreement: an older volume server, or one that has not yet heartbeated
// its shard sizes, must not block an encode that is otherwise sound.
func requireUniformShardSizes(vid needle.VolumeId, byNode map[pb.ServerAddress]*erasure_coding.ShardsInfo) error {
	type holder struct {
		server pb.ServerAddress
		shard  erasure_coding.ShardId
	}
	sizes := make(map[int64][]holder)
	for server, si := range byNode {
		if si == nil {
			continue
		}
		for _, id := range si.Ids() {
			size := int64(si.Size(id))
			if size == 0 {
				continue
			}
			sizes[size] = append(sizes[size], holder{server: server, shard: id})
		}
	}
	if len(sizes) <= 1 {
		return nil
	}

	described := make([]string, 0, len(sizes))
	for size, holders := range sizes {
		sort.Slice(holders, func(i, j int) bool {
			if holders[i].server == holders[j].server {
				return holders[i].shard < holders[j].shard
			}
			return holders[i].server < holders[j].server
		})
		shards := make([]string, 0, len(holders))
		for _, h := range holders {
			shards = append(shards, fmt.Sprintf("%s.%d", h.server, h.shard))
		}
		described = append(described, fmt.Sprintf("%d bytes: %s", size, strings.Join(shards, " ")))
	}
	sort.Strings(described)
	return fmt.Errorf("volume %d ec shards disagree on size, so at least one is incomplete (%s)", vid, strings.Join(described, "; "))
}

func ecShardSummaryByNode(byNode map[pb.ServerAddress]erasure_coding.ShardBits) []string {
	summary := make([]string, 0, len(byNode))
	for node, bits := range byNode {
		summary = append(summary, fmt.Sprintf("%s=%d shards %v", node, bits.Count(), slices.Collect(bits.All())))
	}
	sort.Strings(summary)
	return summary
}

func verifyEcShardsBeforeDelete(env *Env, volumeIds []needle.VolumeId, diskType types.DiskType, expectSpread bool) error {
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
		topoInfo, _, err := env.FetchTopology(0)
		if err != nil {
			return fmt.Errorf("fetch topology for shard verification: %w", err)
		}

		lastErr = nil
		lastDegraded = lastDegraded[:0]
		lastClumped = lastClumped[:0]
		for _, vid := range volumeIds {
			// Count the shards wherever they landed, as waitForEcShardsToRegister
			// above already does. generateEcShards writes them beside the source
			// volume, so encoding a volume that lives on a non-default medium
			// puts them on that medium while -diskType still says hdd. Counting
			// only the -diskType bucket then reports a complete set as entirely
			// missing and aborts an encode that in fact succeeded, leaving the
			// volume as both a .dat and a full set of shards.
			byNode := CollectEcShardBitsByNode(topoInfo, vid)

			var union erasure_coding.ShardBits
			for _, bits := range byNode {
				union |= bits
			}

			totalShards := erasure_coding.TotalShardsCount
			degraded, err := erasure_coding.RequireRecoverableShardSet(uint32(vid), union, erasure_coding.DataShardsCount, totalShards)
			if err != nil {
				lastErr = fmt.Errorf("volume %d: %w (observed: %v)", vid, err, ecShardSummaryByNode(byNode))
				break
			}
			// Counting the shards says they exist, not that they are whole. The
			// source volume is about to be deleted on their word, so also require
			// the sizes to agree -- judging the same generation the count above
			// judged, or an orphaned older encode would veto a healthy set.
			if err := requireUniformShardSizes(vid, collectNewestGenerationShardsInfo(topoInfo, vid)); err != nil {
				lastErr = err
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

			glog.V(0).Infof("EC shard verification ok for volume %d: %d/%d shards present across %d nodes",
				vid, union.Count(), totalShards, len(byNode))
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
func doDeleteVolumesWithLocations(env *Env, volumeIds []needle.VolumeId, volumeLocationsMap map[needle.VolumeId][]wdclient.Location, maxParallelization int) error {
	if !env.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	ewg := util.NewErrorWaitGroup(maxParallelization)
	for _, vid := range volumeIds {
		locations, found := volumeLocationsMap[vid]
		if !found {
			fmt.Printf("warning: no locations found for volume %d, skipping deletion\n", vid)
			continue
		}

		for _, l := range locations {
			ewg.Add(func() error {
				if err := deleteVolume(context.Background(), env.GrpcDialOption, vid, l.ServerAddress(), false, false); err != nil {
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

func SelectVolumeIdsFromTopology(topologyInfo *master_pb.TopologyInfo, volumeSizeLimitMb uint64, collectionRegex *regexp.Regexp, sourceDiskType *types.DiskType, quietSeconds int64, nowUnixSeconds int64, fullPercentage float64, verbose bool) (vids []needle.VolumeId, matchedCollections []string) {
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
	EachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				totalVolumes++

				// ignore remote volumes
				if v.RemoteStorageName != "" {
					remoteVolumes++
					if verbose {
						fmt.Printf("skip volume %d on %s: remote volume (storage: %s)\n",
							v.Id, dn.Id, v.RemoteStorageName)
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
