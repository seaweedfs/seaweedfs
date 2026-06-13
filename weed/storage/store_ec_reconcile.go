package storage

import (
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// datOwnerInfo records both the disk that holds a .dat for a given
// (collection, vid) and the size on disk. The size is consulted by
// pruneIncompleteEcWithSiblingDat before deleting any EC artefacts:
// a zero-byte or truncated .dat is not a credible fallback, and we'd
// rather leave the partial EC in place than wipe it based on garbage.
type datOwnerInfo struct {
	location *DiskLocation
	size     int64
}

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
			// Post-mirror fast path: when the local .ecx is present,
			// mount self-contained against IdxDirectory instead of
			// the owner disk.
			if loc.HasEcxFileOnDisk(key.collection, key.vid) {
				glog.V(0).Infof("ec volume %d (collection=%q): loading orphan shards %v on %s against locally-mirrored sidecars",
					key.vid, key.collection, shards, loc.Directory)
				if err := loc.loadEcShards(shards, key.collection, key.vid, loc.ecShardNotifyHandler); err != nil {
					glog.Errorf("ec volume %d on %s: local-mirror shard load failed: %v", key.vid, loc.Directory, err)
				}
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

// findEcxIdxDirForVolume returns the directory that holds the .ecx file
// for (collection, vid) on this store, scanning every DiskLocation's
// IdxDirectory and (if different) Directory in turn. It is the single-
// volume analogue of indexEcxOwners used by MountEcShards to bridge the
// "shard lives on disk A, .ecx lives on disk B" case at mount time, which
// the per-disk LoadEcShard does not handle on its own.
//
// The first match wins; duplicates across disks are not expected on a
// healthy store, but tolerated for the same reason as indexEcxOwners.
//
// The seen map is hoisted across all locations so a shared IdxDirectory
// (common when a single -dir.idx is paired with multiple -dir entries)
// is only stat'd once per call.
func (s *Store) findEcxIdxDirForVolume(collection string, vid needle.VolumeId) (string, bool) {
	seen := map[string]bool{}
	for _, loc := range s.Locations {
		for _, scan := range []string{loc.IdxDirectory, loc.Directory} {
			if scan == "" || seen[scan] {
				continue
			}
			seen[scan] = true
			base := erasure_coding.EcShardFileName(collection, scan, int(vid))
			// A 0-byte .ecx is not a usable index — EC distribute's writeToFile
			// opens with O_TRUNC and can leave a stub on a mid-stream failure.
			// Treat it the same as absent so the scan continues to a sibling
			// disk that may hold a valid index.
			if info, err := os.Stat(base + ".ecx"); err == nil && !info.IsDir() && info.Size() > 0 {
				return scan, true
			}
		}
	}
	return "", false
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
				// A 0-byte .ecx is a corrupt stub from a failed copy and
				// not a credible owner — skip it so the scan keeps looking
				// for a real index on a sibling disk. Without this, an
				// orphan-shard reconcile could pick the stub as owner and
				// point NewEcVolume at it, which now fails by design
				// (NewEcVolume rejects 0-byte .ecx), leaving the orphan
				// shards unloaded even when a valid index exists nearby.
				info, statErr := entry.Info()
				if statErr != nil || info.Size() == 0 {
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

// pruneIncompleteEcWithSiblingDat removes leftover EC artefacts on one
// disk when a healthy .dat for the same (collection, vid) lives on a
// sibling disk of the same store. This is the cross-disk analogue of the
// validateEcVolume cleanup in handleFoundEcxFile: a same-disk .dat next
// to partial shards is already taken as proof that an EC encode was
// interrupted, and the partial shards get removed so the .dat keeps
// serving the volume. Per-disk loaders cannot see sibling disks, so when
// the .dat ends up on disk A and the partial shards on disk B the per-disk
// pass mistakes the leftover for a normal distributed-EC layout (no .dat
// next to .ecx) and mounts the partial shards. The volume server then
// heartbeats both a regular replica and an EC shard for the same vid, the
// master keeps both entries, and reads route through either path
// depending on the client. Issue 9478.
//
// Cleanup is gated on shardCount < DataShardsCount so that a deliberate
// "full local EC, .dat retained" layout split across two disks (.dat on
// disk A, all 10+ shards on disk B) is left alone — the per-disk loader
// already keeps that configuration when everything is on a single disk,
// and pruning it here would be a behaviour regression for operators who
// rely on it. Distributed EC volumes (no .dat on any disk of this server)
// also fall through unchanged because the lookup in the .dat index below
// will simply not find a match.
//
// The sibling .dat must be a credible encoding source before we delete
// anything: at least the size .vif recorded at encode time, or — when
// unknown (0) — more than a bare superblock so an empty 8-byte stub
// can't pass. A truncated .dat leaves the partial EC alone; those shards
// may still reconstruct from other servers.
//
// We push DeletedEcShardsChan for every pruned shard so the master is told
// to forget the registrations the per-disk pass already emitted on
// NewEcShardsChan during startup, instead of waiting for the first
// periodic heartbeat to reconcile.
// countEcShardsNodeWide returns the number of distinct EC shard ids for
// (collection, vid) held across every disk on this store. A volume's shards
// can be split across sibling disks (e.g. by ec.balance under a shared
// -dir.idx); a per-disk count understates the set and would make a
// node-wide-recoverable volume look like a partial leftover. The caller must
// not hold any DiskLocation.ecVolumesLock when calling this (it takes them).
func (s *Store) countEcShardsNodeWide(collection string, vid needle.VolumeId) int {
	seen := make(map[erasure_coding.ShardId]struct{})
	for _, loc := range s.Locations {
		loc.ecVolumesLock.RLock()
		if ev, ok := loc.ecVolumes[vid]; ok && ev.Collection == collection {
			for _, sh := range ev.Shards {
				seen[sh.ShardId] = struct{}{}
			}
		}
		loc.ecVolumesLock.RUnlock()
	}
	return len(seen)
}

func (s *Store) pruneIncompleteEcWithSiblingDat() {
	if len(s.Locations) < 2 {
		return
	}

	datOwners := s.indexDatOwners()
	if len(datOwners) == 0 {
		return
	}

	for diskId, loc := range s.Locations {
		// Snapshot under the read lock so we are not iterating
		// ecVolumes while the cleanup below takes the write lock.
		type victim struct {
			collection string
			vid        needle.VolumeId
			messages   []*master_pb.VolumeEcShardInformationMessage
			datDir     string
			shardCount int
			dataShards int
		}
		var victims []victim
		loc.ecVolumesLock.RLock()
		for vid, ev := range loc.ecVolumes {
			shardCount := len(ev.Shards)
			// Use the volume's configured data-shard count, not the OSS
			// default: a disk holding a full data set for a custom ratio
			// (e.g. 9 shards of a 9+3 volume) is independently recoverable
			// and must not be mistaken for a partial leftover and wiped.
			dataShards := erasure_coding.DataShardsCount
			if ev.ECContext != nil && ev.ECContext.DataShards > 0 {
				dataShards = ev.ECContext.DataShards
			}
			if shardCount >= dataShards {
				continue
			}
			key := ecKeyForReconcile{collection: ev.Collection, vid: vid}
			owner, hasDat := datOwners[key]
			if !hasDat || owner.location == loc {
				continue
			}
			// Only a byte-exact committed source authorizes deleting these
			// shards: the sibling .dat size must equal the size .vif recorded
			// at encode time. An unknown (0) recorded size, or any mismatch
			// (a stale or truncated .dat), cannot prove the .dat holds this
			// volume's data, so the partial EC is left for distributed
			// reconstruction.
			datFileSize := ev.DatFileSize()
			if datFileSize <= 0 || owner.size != datFileSize {
				glog.Warningf("ec volume %d (collection=%q) on %s has only %d shards; sibling .dat on %s is %d bytes but .vif recorded %d (need byte-exact match); leaving partial EC in place",
					vid, ev.Collection, loc.Directory, shardCount, owner.location.Directory, owner.size, datFileSize)
				continue
			}
			victims = append(victims, victim{
				collection: ev.Collection,
				vid:        vid,
				messages:   ev.ToVolumeEcShardInformationMessage(uint32(diskId)),
				datDir:     owner.location.Directory,
				shardCount: shardCount,
				dataShards: dataShards,
			})
		}
		loc.ecVolumesLock.RUnlock()

		for _, v := range victims {
			// Final node-wide guard: even with a credible sibling .dat, never
			// prune when the volume's shards are independently recoverable
			// across this node's disks (a set split over sibling disks summing
			// to >= dataShards). Those shards may be sole copies of a
			// distributed volume.
			if nodeWide := s.countEcShardsNodeWide(v.collection, v.vid); nodeWide >= v.dataShards {
				glog.Warningf("ec volume %d (collection=%q): %d shards present node-wide (>= %d) are independently recoverable; leaving EC in place despite a sibling .dat",
					v.vid, v.collection, nodeWide, v.dataShards)
				continue
			}
			glog.Warningf("ec volume %d (collection=%q) on %s has only %d shards (need %d) while a byte-exact source .dat exists on sibling disk %s; cleaning up leftover EC files",
				v.vid, v.collection, loc.Directory, v.shardCount, v.dataShards, v.datDir)
			loc.unloadEcVolume(v.vid)
			loc.removeEcVolumeFiles(v.collection, v.vid)
			for _, msg := range v.messages {
				select {
				case s.DeletedEcShardsChan <- msg:
				default:
					// Channel full during startup is fine — the next
					// periodic heartbeat reports the full ecVolumes
					// state, which no longer contains these shards.
					glog.V(2).Infof("DeletedEcShardsChan full while pruning ec volume %d; relying on periodic heartbeat", v.vid)
				}
			}
		}
	}
}

// indexDatOwners returns, for every (collection, vid), the first disk on
// this store that holds a .dat file for it plus the file's size. Used by
// pruneIncompleteEcWithSiblingDat so it can decide whether partial EC
// artefacts on another disk are leftovers of an interrupted encode AND
// whether the sibling .dat is large enough to be a credible fallback.
//
// We record any .dat os.ReadDir can see — including zero-byte shells.
// The mere presence of a .dat means this volume was a regular volume on
// this server at some point, which rules out the "distributed EC, no
// .dat anywhere" reading. Whether that .dat is actually usable is the
// caller's call, made by comparing this size to the EC's recorded
// source size in .vif.
func (s *Store) indexDatOwners() map[ecKeyForReconcile]datOwnerInfo {
	owners := make(map[ecKeyForReconcile]datOwnerInfo)
	for _, loc := range s.Locations {
		entries, err := os.ReadDir(loc.Directory)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if !strings.HasSuffix(name, ".dat") {
				continue
			}
			base := name[:len(name)-len(".dat")]
			collection, vid, err := parseCollectionVolumeId(base)
			if err != nil {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				continue
			}
			key := ecKeyForReconcile{collection: collection, vid: vid}
			if _, exists := owners[key]; !exists {
				owners[key] = datOwnerInfo{location: loc, size: info.Size()}
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
