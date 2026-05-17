package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// ecMirroredSidecars is the set of EC metadata files that must travel
// with the shards. Each disk that holds .ec?? files for a
// (collection, vid) on this volume server is expected to also hold a
// local copy of these files so the EcVolume can mount self-contained —
// without reaching across to a sibling disk for its index, journal, or
// volume info.
//
// `.ecx` is the sorted needle index produced at encode time.
// `.ecj` is the deletion journal appended on every blob delete.
// `.vif` is the VolumeInfo (version, datFileSize, EC ratio, ttl).
//
// Listed in the order NewEcVolume opens them at mount, so a partial
// mirror can be diagnosed by walking the list and stopping at the
// first destination that does not yet have its copy.
var ecMirroredSidecars = []string{".ecx", ".ecj", ".vif"}

// mirrorEcMetadataToShardDisks copies the EC sidecar files (.ecx,
// .ecj, .vif) onto every disk of this store that holds EC shards but
// is missing the matching sidecars. The goal is the same-disk
// invariant promised by the EC lifecycle: encode / decode / balance /
// vacuum / repair must leave every shard alongside its own metadata,
// so the EcVolume can mount without reaching across to a sibling
// disk.
//
// Without this pass, a disk that received shards through ec.balance
// or ec.rebuild — where only the first shard carries
// CopyEcxFile=true and subsequent shards land via auto-select — can
// end up with the .ec?? files but no local .ecx / .ecj / .vif. The
// orphan-shard reconciler in reconcileEcShardsAcrossDisks then mounts
// the shards by pointing the EcVolume at the sibling disk's index
// files. Reads work, but any failure on the index-owning disk (a
// removed drive, a corrupted ext4, an operator who unmounted the
// wrong sled) takes the shards with it.
//
// Mirroring physically replicates the sidecars onto each
// shard-bearing disk so the shards stay readable as long as their own
// disk is up. Idempotent: a destination that already has its sidecar
// is skipped, and a missing source sidecar is not an error.
//
// Runs before reconcileEcShardsAcrossDisks so the orphan-shard pass
// sees the freshly-mirrored layout and can mount each disk against
// its own IdxDirectory rather than falling back to the cross-disk
// path. The cross-disk fallback is preserved for cases where
// mirroring fails (out of disk space, read-only target, etc.) so the
// volume stays available.
func (s *Store) mirrorEcMetadataToShardDisks() {
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
		for key := range orphans {
			owner, ok := ecxOwners[key]
			if !ok {
				continue
			}
			// Same-disk owner: the per-disk loader will have already
			// surfaced any failure. Mirroring to the same disk is a
			// no-op anyway (source == destination).
			if owner.location == loc {
				continue
			}
			// Skip if this destination already has every sidecar — common
			// when mirroring ran successfully on a previous startup and
			// the operator hasn't disturbed the layout.
			if loc.hasAllEcSidecarsLocally(key.collection, key.vid) {
				continue
			}
			copied, err := loc.mirrorEcSidecarsFrom(owner, key.collection, key.vid)
			if err != nil {
				// Don't abort the whole pass on a single failure: the
				// cross-disk reconciler downstream still gives this
				// volume a working (if non-mirrored) mount.
				glog.Warningf("ec volume %d (collection=%q): mirror sidecars from %s to %s failed after %d files: %v; cross-disk fallback will handle this volume",
					key.vid, key.collection, owner.location.Directory, loc.Directory, copied, err)
				continue
			}
			if copied > 0 {
				glog.V(0).Infof("ec volume %d (collection=%q): mirrored %d sidecar(s) from %s to %s for same-disk invariant",
					key.vid, key.collection, copied, owner.location.Directory, loc.Directory)
			}
		}
	}
}

// hasAllEcSidecarsLocally reports whether this disk already has every
// EC sidecar ecMirroredSidecars expects. Checks the modern routing
// first (IdxDirectory for .ecx/.ecj, Directory for .vif) and then the
// opposite directory as a fallback — matching NewEcVolume's own
// open-with-fallback contract and HasEcxFileOnDisk. Without the
// fallback, a destination that still has a legacy pre-`-dir.idx`
// .ecx in its data dir would be re-mirrored into IdxDirectory and
// end up with two copies on disk.
func (l *DiskLocation) hasAllEcSidecarsLocally(collection string, vid needle.VolumeId) bool {
	for _, ext := range ecMirroredSidecars {
		primary := l.ecSidecarDestPath(collection, vid, ext)
		if statRegular(primary) {
			continue
		}
		// Fall back to the opposite directory for the legacy layout:
		// .ecx/.ecj could be in Directory (pre-`-dir.idx`), .vif
		// could be in IdxDirectory (uncommon but allowed by
		// NewEcVolume's .vif lookup).
		if l.IdxDirectory != l.Directory {
			fallbackDir := l.Directory
			if ext == ".vif" {
				fallbackDir = l.IdxDirectory
			}
			fallback := erasure_coding.EcShardFileName(collection, fallbackDir, int(vid)) + ext
			if statRegular(fallback) {
				continue
			}
		}
		return false
	}
	return true
}

// statRegular returns true iff path exists and is a regular file
// (not a directory). Used by the sidecar-presence checks; keeps the
// fallback ladder readable.
func statRegular(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// ecSidecarDestPath returns where on this disk a given EC sidecar
// extension is expected to live. Keeps the routing logic close to
// NewEcVolume's lookup order:
//
//	.ecx / .ecj → IdxDirectory (sorted index, deletion journal)
//	.vif        → Directory   (volume info next to .dat / shards)
func (l *DiskLocation) ecSidecarDestPath(collection string, vid needle.VolumeId, ext string) string {
	if ext == ".vif" {
		return erasure_coding.EcShardFileName(collection, l.Directory, int(vid)) + ext
	}
	return erasure_coding.EcShardFileName(collection, l.IdxDirectory, int(vid)) + ext
}

// mirrorEcSidecarsFrom copies every sidecar in ecMirroredSidecars
// from the owner disk to this disk. Returns the count of files
// successfully copied — a non-zero count with a non-nil error
// indicates a partial mirror, which the caller logs but otherwise
// leaves to the cross-disk fallback.
//
// Source resolution per file: check the owner's recorded idxDir first
// (matches indexEcxOwners' "where I actually found .ecx" record),
// then fall back to the owner's data Directory. This matches
// removeEcVolumeFiles' two-directory cleanup contract — we read from
// wherever the owner actually put the file.
//
// Each copy is atomic: write to <dst>.tmp, fsync, rename. A
// pre-existing destination is left untouched (no overwrite), so a
// half-finished mirror from a previous startup is healed file-by-file
// without rewriting bytes that are already correct.
func (l *DiskLocation) mirrorEcSidecarsFrom(owner ecxOwnerInfo, collection string, vid needle.VolumeId) (int, error) {
	srcIdxBase := erasure_coding.EcShardFileName(collection, owner.idxDir, int(vid))
	srcDataBase := erasure_coding.EcShardFileName(collection, owner.location.Directory, int(vid))

	copied := 0
	for _, ext := range ecMirroredSidecars {
		dst := l.ecSidecarDestPath(collection, vid, ext)
		// Don't overwrite a sidecar that's already present: an
		// existing local copy is authoritative (it may be newer than
		// the owner's after a delete journal append).
		if _, err := os.Stat(dst); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return copied, fmt.Errorf("stat %s: %w", dst, err)
		}

		// Pick the source: idxDir first (where the owner reported
		// finding .ecx), then the owner's data dir as a fallback for
		// .vif (and for legacy layouts that left .ecx in Directory).
		var src string
		for _, candidate := range []string{srcIdxBase + ext, srcDataBase + ext} {
			if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
				src = candidate
				break
			}
		}
		if src == "" {
			glog.V(1).Infof("ec volume %d (collection=%q): sidecar %s not found on owner %s; skipping mirror",
				vid, collection, ext, owner.location.Directory)
			continue
		}

		if err := copyEcSidecarAtomic(src, dst); err != nil {
			return copied, fmt.Errorf("copy %s: %w", ext, err)
		}
		copied++
	}
	return copied, nil
}

// copyEcSidecarAtomic copies src to dst via a sibling .tmp file and a
// rename, fsyncing the data before the rename. Crash-safe: a partial
// write leaves the .tmp orphaned (cleaned up on the next mirror pass
// via the os.Remove on failure) and the canonical dst stays absent so
// a retry recognises it still needs to copy.
//
// Caller has already verified dst does not exist; we still pass
// O_EXCL on the tmp create so a concurrent mirror can't both fight
// for the same temp path.
func copyEcSidecarAtomic(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source %s: %w", src, err)
	}
	defer srcFile.Close()

	// Make sure the destination directory exists. Locations created
	// before -dir.idx was set may not have the idx tree yet on a
	// fresh disk; MkdirAll is a no-op when it already does.
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}

	tmpDst := dst + ".mirror.tmp"
	// O_EXCL guards against a stale tmp from a previous interrupted
	// run; if it's there, blow it away and retry.
	_ = os.Remove(tmpDst)
	dstFile, err := os.OpenFile(tmpDst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmpDst, err)
	}
	cleanup := func() {
		_ = dstFile.Close()
		_ = os.Remove(tmpDst)
	}

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		cleanup()
		return fmt.Errorf("copy bytes %s -> %s: %w", src, tmpDst, err)
	}
	if err := dstFile.Sync(); err != nil {
		cleanup()
		return fmt.Errorf("fsync %s: %w", tmpDst, err)
	}
	if err := dstFile.Close(); err != nil {
		_ = os.Remove(tmpDst)
		return fmt.Errorf("close %s: %w", tmpDst, err)
	}
	if err := os.Rename(tmpDst, dst); err != nil {
		_ = os.Remove(tmpDst)
		return fmt.Errorf("rename %s -> %s: %w", tmpDst, dst, err)
	}
	return nil
}
