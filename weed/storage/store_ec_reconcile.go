package storage

import (
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// ecKeyForReconcile keys orphan-shard reconciliation by collection + volume
// id. Per-collection grouping matters because two collections can re-use the
// same volume id, and we must only pair shards with their own .ecx file.
type ecKeyForReconcile struct {
	collection string
	vid        needle.VolumeId
}

// ecxOwnerInfo records both the disk that owns the .ecx and the actual
// directory it lives in (IdxDirectory or Directory). The directory matters
// because indexEcxOwners scans both — when .ecx lives in Directory (the
// legacy "written before -dir.idx was set" layout that removeEcVolumeFiles
// in disk_location_ec.go also keeps cleaning up), passing the owner's
// IdxDirectory to NewEcVolume would ENOENT both the primary and the
// same-disk fallback path, which uses the orphan disk's data dir, not the
// owner's. Tracking the actual scan dir lets reconcile point loaders at
// the directory the .ecx is really in.
type ecxOwnerInfo struct {
	location *DiskLocation
	idxDir   string
}

// reconcileEcShardsAcrossDisks loads EC shards that the per-disk scan in
// loadAllEcShards skipped because the disk holding the .ec?? files does not
// also hold the matching .ecx / .ecj / .vif index files. The index files
// are located on a different disk of the same volume server (issue #9212).
//
// Per-disk loadAllEcShards correctly leaves these orphan shards on disk —
// it does not have visibility into other DiskLocations on the same store —
// so the cross-disk fan-out must happen here, after every disk's initial
// pass has completed. We register each shard against its physical disk's
// ecVolumes map (so heartbeat reporting carries the right DiskId per
// shard), but point the EcVolume at the sibling disk's index files so it
// can serve reads and route deletes through a real .ecx / .ecj.
func (s *Store) reconcileEcShardsAcrossDisks() {
	if len(s.Locations) < 2 {
		return
	}

	ecxOwners := s.indexEcxOwners()
	if len(ecxOwners) == 0 {
		return
	}

	for _, loc := range s.Locations {
		orphans := loc.collectOrphanEcShards()
		if len(orphans) == 0 {
			continue
		}
		for key, shards := range orphans {
			owner, ok := ecxOwners[key]
			if !ok {
				glog.Warningf("ec volume %d (collection=%q) has shards on %s without a matching .ecx anywhere on this volume server; shards %v will stay unloaded until the missing .ecx is restored",
					key.vid, key.collection, loc.Directory, shards)
				continue
			}
			if owner.location == loc {
				// .ecx is on this same disk, but loadAllEcShards still
				// did not load these shards — handleFoundEcxFile already
				// logged the underlying failure. Don't try again here.
				continue
			}
			glog.V(0).Infof("ec volume %d (collection=%q): loading orphan shards %v on %s using index files from %s (issue #9212)",
				key.vid, key.collection, shards, loc.Directory, owner.idxDir)
			if err := loc.loadEcShardsWithIdxDir(shards, key.collection, key.vid, owner.idxDir, loc.ecShardNotifyHandler); err != nil {
				glog.Errorf("ec volume %d on %s: cross-disk shard load failed: %v", key.vid, loc.Directory, err)
			}
		}
	}
}

// indexEcxOwners returns the disk and the actual directory that owns the
// .ecx file for each (collection, vid) on this store. .ecx normally lives
// in IdxDirectory but may have been written into the data directory before
// -dir.idx was set, so we check both — and we record which one matched so
// downstream loaders point NewEcVolume at the directory that really has
// the file. The first owner found wins; duplicates across disks are
// unusual but tolerated.
func (s *Store) indexEcxOwners() map[ecKeyForReconcile]ecxOwnerInfo {
	owners := make(map[ecKeyForReconcile]ecxOwnerInfo)
	for _, loc := range s.Locations {
		seen := make(map[string]bool, 2)
		for _, scan := range []string{loc.IdxDirectory, loc.Directory} {
			if scan == "" || seen[scan] {
				continue
			}
			seen[scan] = true
			entries, err := os.ReadDir(scan)
			if err != nil {
				continue
			}
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				name := entry.Name()
				if !strings.HasSuffix(name, ".ecx") {
					continue
				}
				base := name[:len(name)-len(".ecx")]
				collection, vid, err := parseCollectionVolumeId(base)
				if err != nil {
					continue
				}
				key := ecKeyForReconcile{collection: collection, vid: vid}
				if _, exists := owners[key]; !exists {
					owners[key] = ecxOwnerInfo{location: loc, idxDir: scan}
				}
			}
		}
	}
	return owners
}

// collectOrphanEcShards walks the disk's data directory and returns the
// .ec?? shard files that are present on disk but not yet registered to an
// EcVolume in memory. The map is keyed by (collection, vid) so callers can
// match each group against the .ecx-owning disk in one lookup.
//
// Zero-byte shard files are ignored — loadAllEcShards already treats them
// as cleanup-worthy noise and we want the same shape here.
func (l *DiskLocation) collectOrphanEcShards() map[ecKeyForReconcile][]string {
	entries, err := os.ReadDir(l.Directory)
	if err != nil {
		return nil
	}
	orphans := make(map[ecKeyForReconcile][]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		ext := path.Ext(name)
		if !re.MatchString(ext) {
			continue
		}
		info, err := entry.Info()
		if err != nil || info.Size() == 0 {
			continue
		}
		shardId, err := strconv.ParseInt(ext[3:], 10, 64)
		if err != nil || shardId < 0 || shardId > 255 {
			continue
		}
		base := name[:len(name)-len(ext)]
		collection, vid, err := parseCollectionVolumeId(base)
		if err != nil {
			continue
		}
		if _, loaded := l.FindEcShard(vid, erasure_coding.ShardId(shardId)); loaded {
			continue
		}
		key := ecKeyForReconcile{collection: collection, vid: vid}
		orphans[key] = append(orphans[key], name)
	}
	return orphans
}
