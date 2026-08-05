package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// errStopIdxWalk ends an idx.WalkIndexFile early once the answer is known.
var errStopIdxWalk = errors.New("stop idx walk")

// repairIdxHeadTombstones restores the .idx rows that deletes on a tiered
// read-only volume used to overwrite.
//
// The sorted-file needle map opened .idx read-write but never seeded its write
// position, so a delete wrote its (key, offset 0, tombstone) row at .idx offset
// 0 and advanced one row at a time instead of appending. Every delete therefore
// replaced one more row at the front of .idx, and the rows it replaced -- the
// Put rows indexing the first needles in .dat -- were lost. Reads for those
// needles return not-found even though .dat still holds them intact.
//
// The damage leaves a fingerprint: .idx begins with a run of offset-0
// tombstones. A healthy .idx never does. Its first row is the Put for the first
// needle in .dat, and an offset-0 tombstone -- a delete against a tiered volume,
// which appends no .dat record and so has no extent to point at -- can only ever
// land at the tail.
//
// .idx and .dat grow in lockstep, so the clobbered rows indexed exactly the
// first N records of .dat. Re-deriving them is a header-only walk over the head
// of .dat, which stays cheap even when .dat is served from a remote tier.
//
// The recovered rows go back in front, where the rows they replace used to sit,
// and every existing row keeps its relative order behind them. That restores
// .idx to .dat append order, which several readers lean on: the fingerprint is
// gone so a later load stops after one row, the last row is the .dat-tail needle
// again so CheckVolumeDataIntegrity keeps its O(1) path, and
// BinarySearchByAppendAtNs sees ascending append timestamps.
func (v *Volume) repairIdxHeadTombstones() (restored int, err error) {
	if v.DataBackend == nil {
		return 0, nil
	}
	version := v.Version()
	if !needle.IsSupportedVersion(version) {
		return 0, nil
	}
	firstNeedleOffset := int64(v.SuperBlock.BlockSize())
	idxFileName := v.FileName(".idx")

	clobbered, err := idxHeadTombstoneCount(idxFileName)
	if err != nil || clobbered == 0 {
		return 0, err
	}

	glog.V(0).Infof("volume %d: %s starts with %d offset-0 tombstones, recovering the .idx rows they overwrote from %s",
		v.Id, idxFileName, clobbered, v.FileName(".dat"))

	lost, order, err := scanDatHead(v.DataBackend, version, firstNeedleOffset, clobbered)
	if err != nil {
		return 0, err
	}
	if err = dropIndexedKeys(idxFileName, lost); err != nil {
		return 0, err
	}
	if len(lost) == 0 {
		return 0, nil
	}

	var rows []byte
	for _, key := range order {
		nv, ok := lost[key]
		if !ok {
			continue
		}
		rows = append(rows, nv.ToBytes()...)
		restored++
	}
	return restored, prependIdxRows(idxFileName, rows)
}

// idxHeadTombstoneCount reports how many rows at the front of .idx are offset-0
// tombstones. A healthy .idx answers 0 on its first row.
func idxHeadTombstoneCount(idxFileName string) (clobbered int, err error) {
	idxFile, err := os.Open(idxFileName)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer idxFile.Close()

	err = idx.WalkIndexFile(idxFile, 0, func(_ types.NeedleId, offset types.Offset, size types.Size) error {
		if !offset.IsZero() || !size.IsTombstone() {
			return errStopIdxWalk
		}
		clobbered++
		return nil
	})
	if errors.Is(err, errStopIdxWalk) {
		err = nil
	}
	return clobbered, err
}

// scanDatHead reads the headers of the first limit records of .dat and returns
// the needles they hold, in .dat order. A record with an invalid size is a
// delete marker, so the key it names drops out of the result rather than being
// resurrected.
func scanDatHead(datBackend backend.BackendStorageFile, version needle.Version, firstNeedleOffset int64, limit int) (map[types.NeedleId]needle_map.NeedleValue, []types.NeedleId, error) {
	scanner := &datHeadScanner{
		limit: limit,
		found: make(map[types.NeedleId]needle_map.NeedleValue),
	}
	if err := ScanVolumeFileFrom(version, datBackend, firstNeedleOffset, scanner); err != nil {
		return nil, nil, fmt.Errorf("scan head of %s: %w", datBackend.Name(), err)
	}
	return scanner.found, scanner.order, nil
}

type datHeadScanner struct {
	limit   int
	visited int
	found   map[types.NeedleId]needle_map.NeedleValue
	order   []types.NeedleId
}

func (s *datHeadScanner) VisitSuperBlock(super_block.SuperBlock) error { return nil }

func (s *datHeadScanner) ReadNeedleBody() bool { return false }

func (s *datHeadScanner) VisitNeedle(n *needle.Needle, offset int64, _, _ []byte) error {
	if n.Size.IsValid() {
		if _, seen := s.found[n.Id]; !seen {
			s.order = append(s.order, n.Id)
		}
		s.found[n.Id] = needle_map.NeedleValue{Key: n.Id, Offset: types.ToOffset(offset), Size: n.Size}
	} else {
		delete(s.found, n.Id)
	}
	s.visited++
	if s.visited >= s.limit {
		return io.EOF
	}
	return nil
}

// dropIndexedKeys removes every candidate the .idx already names, whether by a
// Put row or a tombstone. What is left is only what the clobbered rows held.
func dropIndexedKeys(idxFileName string, candidates map[types.NeedleId]needle_map.NeedleValue) error {
	if len(candidates) == 0 {
		return nil
	}
	idxFile, err := os.Open(idxFileName)
	if err != nil {
		return err
	}
	defer idxFile.Close()

	err = idx.WalkIndexFile(idxFile, 0, func(key types.NeedleId, _ types.Offset, _ types.Size) error {
		delete(candidates, key)
		if len(candidates) == 0 {
			return errStopIdxWalk
		}
		return nil
	})
	if errors.Is(err, errStopIdxWalk) {
		err = nil
	}
	return err
}

// prependIdxRows rewrites .idx as rows followed by its current contents,
// through a temp file and a rename so a crash mid-write never leaves a partial
// index at the live name.
func prependIdxRows(idxFileName string, rows []byte) error {
	src, err := os.Open(idxFileName)
	if err != nil {
		return err
	}
	defer src.Close()

	srcStat, err := src.Stat()
	if err != nil {
		return err
	}

	tmpFileName := idxFileName + ".tmp"
	dst, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, srcStat.Mode().Perm())
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		dst.Close()
		if !committed {
			os.Remove(tmpFileName)
		}
	}()

	// The rename replaces .idx with this file, so it has to carry the mode the
	// index already had -- O_CREATE alone leaves it at the umask's mercy.
	if err = dst.Chmod(srcStat.Mode().Perm()); err != nil {
		return err
	}
	if _, err = dst.Write(rows); err != nil {
		return err
	}
	if _, err = io.Copy(dst, src); err != nil {
		return err
	}
	if err = dst.Sync(); err != nil {
		return err
	}
	if err = dst.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpFileName, idxFileName); err != nil {
		return err
	}
	if err = util.FsyncDir(filepath.Dir(idxFileName)); err != nil {
		return err
	}
	committed = true
	return nil
}
