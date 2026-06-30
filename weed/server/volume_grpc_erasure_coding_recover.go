package weed_server

import (
	"context"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

// ecRecoveryLookupTimeout bounds the master LookupEcVolume call so a slow or
// unresponsive master cannot hang the synchronous recovery RPC.
const ecRecoveryLookupTimeout = time.Minute

// recoverMissingEcIndexes fetches the .ecx / .ecj / .vif index files for EC
// volumes whose shards sit on this server while the index lives only on a peer,
// then mounts the now-recoverable shards. This self-heals the "shards present,
// .ecx missing everywhere local" layout (issue #10104) that the per-disk loader
// and the same-server cross-disk reconcile both leave unmounted, including for
// volumes already broken before the upgrade.
//
// filterVid limits recovery to a single volume id; 0 recovers every orphan on
// this server (used by ec.rebuild to heal volumes the master never learned
// about). It is driven on demand by VolumeEcShardsMount (recover_missing_index),
// so an operator triggers it through ec.rebuild rather than a background loop.
// Returns the number of volumes whose index was recovered.
func (vs *VolumeServer) recoverMissingEcIndexes(filterVid uint32) int {
	orphans := vs.store.CollectEcVolumesMissingIndex()
	if filterVid != 0 {
		filtered := orphans[:0]
		for _, m := range orphans {
			if uint32(m.VolumeId) == filterVid {
				filtered = append(filtered, m)
			}
		}
		orphans = filtered
	}
	if len(orphans) == 0 {
		return 0
	}

	master := vs.getCurrentMaster()
	if master == "" {
		glog.Warningf("cannot recover missing EC index without a master connection")
		return 0
	}
	self := pb.NewServerAddress(vs.store.Ip, vs.store.Port, vs.store.GrpcPort)

	recovered := 0
	for _, m := range orphans {
		peers, err := vs.lookupEcVolumePeers(master, uint32(m.VolumeId), self)
		if err != nil {
			glog.Warningf("ec volume %d: cannot look up peers to recover missing .ecx: %v", m.VolumeId, err)
			continue
		}
		if len(peers) == 0 {
			glog.Warningf("ec volume %d: shards present locally but .ecx missing and no peer holds it; leaving shards unloaded", m.VolumeId)
			continue
		}
		if vs.fetchEcIndexFromPeers(peers, m) {
			recovered++
		}
	}

	if recovered > 0 {
		vs.store.MountRecoveredEcShards()
		glog.V(0).Infof("recovered missing EC index for %d volume(s) from peers and mounted their shards", recovered)
	}
	return recovered
}

// lookupEcVolumePeers asks the master which servers hold shards for vid and
// returns the unique peer addresses, excluding this server itself.
func (vs *VolumeServer) lookupEcVolumePeers(master pb.ServerAddress, vid uint32, self pb.ServerAddress) ([]pb.ServerAddress, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ecRecoveryLookupTimeout)
	defer cancel()

	var peers []pb.ServerAddress
	seen := make(map[pb.ServerAddress]bool)
	err := operation.WithMasterServerClient(ctx, false, master, vs.grpcDialOption, func(client master_pb.SeaweedClient) error {
		resp, err := client.LookupEcVolume(ctx, &master_pb.LookupEcVolumeRequest{VolumeId: vid})
		if err != nil {
			return err
		}
		for _, shardIdLocations := range resp.ShardIdLocations {
			for _, loc := range shardIdLocations.Locations {
				addr := pb.NewServerAddressFromLocation(loc)
				if addr.Equals(self) || seen[addr] {
					continue
				}
				seen[addr] = true
				peers = append(peers, addr)
			}
		}
		return nil
	})
	return peers, err
}

// fetchEcIndexFromPeers tries each peer in turn, copying the .ecx (required) and
// the .ecj / .vif (best-effort) for the volume onto the local disk recorded in
// m. The .ecx is an immutable encode-time index, identical on every holder, so
// any peer's copy serves. The .ecj is a per-holder deletion journal that differs
// across holders (a delete is journaled on only one node); the recovered node
// adopts the source peer's deletion view, exactly as a balanced or rebuilt shard
// does — the EC delete model already tolerates that divergence. The first peer
// that yields a non-empty .ecx wins; a peer with no index or a 0-byte stub is
// skipped (the orphan shards are non-empty, so a 0-byte index cannot be theirs).
// A failed copy removes its partial file so a later attempt is not blocked by a
// stub.
func (vs *VolumeServer) fetchEcIndexFromPeers(peers []pb.ServerAddress, m storage.EcVolumeMissingIndex) bool {
	idxBaseFileName := storage.VolumeFileName(m.IdxDir, m.Collection, int(m.VolumeId))
	dataBaseFileName := storage.VolumeFileName(m.DataDir, m.Collection, int(m.VolumeId))
	ecxPath := idxBaseFileName + ".ecx"
	ecjPath := idxBaseFileName + ".ecj"

	removePartial := func(path string) {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			glog.Warningf("ec volume %d: remove partial %s: %v", m.VolumeId, path, err)
		}
	}

	for _, peer := range peers {
		err := operation.WithVolumeServerClient(true, peer, vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
			// .ecx is mandatory; a peer without it errors and we move on.
			if _, err := vs.doCopyFile(client, true, m.Collection, uint32(m.VolumeId), math.MaxUint32, math.MaxInt64, idxBaseFileName, ".ecx", false, false, nil); err != nil {
				removePartial(ecxPath)
				return err
			}
			info, statErr := os.Stat(ecxPath)
			if statErr != nil {
				removePartial(ecxPath)
				return fmt.Errorf("stat copied .ecx %s: %w", ecxPath, statErr)
			}
			if info.IsDir() || info.Size() == 0 {
				removePartial(ecxPath)
				return fmt.Errorf("peer %s served an unusable .ecx (size %d)", peer, info.Size())
			}
			// .ecj is the source peer's deletion journal; .vif carries EC params
			// and EncodeTsNs. Both are best-effort: a missing .ecj is recreated at
			// mount and a missing .vif falls back to default EC parameters. A failed
			// .ecj copy is an in-place append, so drop the partial file; the .vif
			// copy stages and renames, leaving nothing to clean up.
			if _, err := vs.doCopyFile(client, true, m.Collection, uint32(m.VolumeId), math.MaxUint32, math.MaxInt64, idxBaseFileName, ".ecj", true, true, nil); err != nil {
				glog.Warningf("ec volume %d: copy .ecj from %s: %v", m.VolumeId, peer, err)
				removePartial(ecjPath)
			}
			if _, err := vs.doCopyFile(client, true, m.Collection, uint32(m.VolumeId), math.MaxUint32, math.MaxInt64, dataBaseFileName, ".vif", false, true, nil); err != nil {
				glog.Warningf("ec volume %d: copy .vif from %s: %v", m.VolumeId, peer, err)
			}
			return nil
		})
		if err != nil {
			glog.V(1).Infof("ec volume %d: fetch missing .ecx from %s failed: %v", m.VolumeId, peer, err)
			continue
		}
		glog.V(0).Infof("ec volume %d: fetched missing .ecx from %s into %s", m.VolumeId, peer, m.IdxDir)
		return true
	}
	return false
}
