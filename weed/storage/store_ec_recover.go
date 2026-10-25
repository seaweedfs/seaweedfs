package storage

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// EcVolumeMissingIndex names an EC volume whose .ec?? shard files are present on
// this store but whose .ecx index is missing on every local disk, so the shards
// cannot be mounted. The per-disk loader and the same-server cross-disk reconcile
// both leave such shards unmounted because the index lives only on a different
// server (issue #10104). The recovery path fetches the index from a peer that
// still holds it and drops it into IdxDir / DataDir, then re-runs the mount.
type EcVolumeMissingIndex struct {
	Collection string
	VolumeId   needle.VolumeId
	IdxDir     string // destination for the fetched .ecx / .ecj
	DataDir    string // destination for the fetched .vif
}

// CollectEcVolumesMissingIndex returns every EC volume that has shard files on a
// local disk but no usable .ecx on any local disk — the cross-server orphans a
// peer index fetch must recover. Volumes whose index merely sits on a sibling
// disk are excluded (the same-server reconcile/mirror handles those). The
// destination dirs are taken from the first disk found holding orphan shards;
// MountRecoveredEcShards mirrors the index onto the remaining shard-bearing disks.
//
// It scans on-disk shard files directly, so it surfaces volumes the master never
// learned about — including those whose every holder is missing its index.
func (s *Store) CollectEcVolumesMissingIndex() []EcVolumeMissingIndex {
	ecxOwners := s.indexEcxOwners()

	seen := make(map[ecKeyForReconcile]bool)
	var missing []EcVolumeMissingIndex
	for _, loc := range s.Locations {
		for key := range loc.collectOrphanEcShards() {
			if _, hasLocalIndex := ecxOwners[key]; hasLocalIndex {
				continue
			}
			if seen[key] {
				continue
			}
			seen[key] = true
			missing = append(missing, EcVolumeMissingIndex{
				Collection: key.collection,
				VolumeId:   key.vid,
				IdxDir:     loc.IdxDirectory,
				DataDir:    loc.Directory,
			})
		}
	}
	return missing
}

// MountRecoveredEcShards mounts EC shards that became loadable after a missing
// .ecx was fetched onto a local disk. It mirrors the index onto every
// shard-bearing disk, mounts the disks that now have a local index, and falls
// back to the cross-disk virtual mount for any disk the mirror could not reach.
// Each shard load announces itself through ecShardNotifyHandler so the master
// learns about the now-registered shards.
func (s *Store) MountRecoveredEcShards() {
	s.mirrorEcMetadataToShardDisks()
	s.loadOrphanEcShardsWithLocalIndex()
	s.reconcileEcShardsAcrossDisks()
}

// loadOrphanEcShardsWithLocalIndex mounts on-disk EC shards whose .ecx index is
// now present on the same disk. Unlike reconcileEcShardsAcrossDisks it does not
// require a sibling disk, so a single-disk store recovers too once its index has
// been fetched from a peer.
func (s *Store) loadOrphanEcShardsWithLocalIndex() {
	for _, loc := range s.Locations {
		orphans := loc.collectOrphanEcShards()
		for key, shards := range orphans {
			if !loc.HasEcxFileOnDisk(key.collection, key.vid) {
				continue
			}
			if err := loc.loadEcShards(shards, key.collection, key.vid, loc.ecShardNotifyHandler); err != nil {
				glog.Errorf("ec volume %d on %s: load after index recovery failed: %v", key.vid, loc.Directory, err)
			}
		}
	}
}
