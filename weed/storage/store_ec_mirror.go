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

// Listed in the order NewEcVolume opens them.
var ecMirroredSidecars = []string{".ecx", ".ecj", ".vif"}

// mirrorEcMetadataToShardDisks physically copies .ecx / .ecj / .vif
// onto every disk that holds EC shards but lacks the matching
// sidecars, so each shard-bearing disk mounts self-contained instead
// of reaching across to a sibling. Runs before
// reconcileEcShardsAcrossDisks; the cross-disk virtual mount stays
// as the fallback when mirroring fails.
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
			if owner.location == loc {
				continue
			}
			if loc.hasAllEcSidecarsLocally(key.collection, key.vid) {
				continue
			}
			copied, err := loc.mirrorEcSidecarsFrom(owner, key.collection, key.vid)
			if err != nil {
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

// hasAllEcSidecarsLocally checks both the modern routing and the
// opposite directory (legacy pre-`-dir.idx` layout) — without the
// fallback, a destination that still has .ecx in its data dir would
// be re-mirrored into IdxDirectory.
func (l *DiskLocation) hasAllEcSidecarsLocally(collection string, vid needle.VolumeId) bool {
	for _, ext := range ecMirroredSidecars {
		if statRegular(l.ecSidecarDestPath(collection, vid, ext)) {
			continue
		}
		if l.IdxDirectory != l.Directory {
			fallbackDir := l.Directory
			if ext == ".vif" {
				fallbackDir = l.IdxDirectory
			}
			if statRegular(erasure_coding.EcShardFileName(collection, fallbackDir, int(vid)) + ext) {
				continue
			}
		}
		return false
	}
	return true
}

func statRegular(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// ecSidecarDestPath routes .ecx/.ecj to IdxDirectory and .vif to
// Directory, matching NewEcVolume's open order.
func (l *DiskLocation) ecSidecarDestPath(collection string, vid needle.VolumeId, ext string) string {
	if ext == ".vif" {
		return erasure_coding.EcShardFileName(collection, l.Directory, int(vid)) + ext
	}
	return erasure_coding.EcShardFileName(collection, l.IdxDirectory, int(vid)) + ext
}

func (l *DiskLocation) mirrorEcSidecarsFrom(owner ecxOwnerInfo, collection string, vid needle.VolumeId) (int, error) {
	srcIdxBase := erasure_coding.EcShardFileName(collection, owner.idxDir, int(vid))
	srcDataBase := erasure_coding.EcShardFileName(collection, owner.location.Directory, int(vid))

	copied := 0
	for _, ext := range ecMirroredSidecars {
		dst := l.ecSidecarDestPath(collection, vid, ext)
		// An existing local copy is authoritative — it may be newer
		// than the owner's after a delete journal append.
		if _, err := os.Stat(dst); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return copied, fmt.Errorf("stat %s: %w", dst, err)
		}

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

// copyEcSidecarAtomic writes to <dst>.mirror.tmp, fsyncs, renames.
// Crash-safe: a partial write leaves the tmp orphaned and the
// canonical dst absent so retries recognise the file still needs
// copying.
func copyEcSidecarAtomic(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source %s: %w", src, err)
	}
	defer srcFile.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}

	tmpDst := dst + ".mirror.tmp"
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
