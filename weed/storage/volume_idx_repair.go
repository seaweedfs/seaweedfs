package storage

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle_map"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
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
// of .dat, which stays cheap even when .dat is served from a remote tier. The
// recovered rows are appended, so the surviving tail and the tombstones
// themselves are left alone.
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

	clobbered, headIndexed, err := scanIdxHeadTombstones(idxFileName, firstNeedleOffset)
	if err != nil || clobbered == 0 || headIndexed {
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
	return restored, appendIdxRows(idxFileName, rows)
}

// scanIdxHeadTombstones reports how many rows at the front of .idx are offset-0
// tombstones, and whether any row still indexes the first needle in .dat.
//
// headIndexed is the cheap "already recovered" signal: once the row for the
// first .dat record is back, later loads skip the .dat walk. It stays false in
// the corner case where that record's key was overwritten later in the volume's
// life -- the key is then indexed by the newer row, nothing needs restoring, and
// the only cost is repeating the (bounded) .dat head walk on each load.
func scanIdxHeadTombstones(idxFileName string, firstNeedleOffset int64) (clobbered int, headIndexed bool, err error) {
	idxFile, err := os.Open(idxFileName)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	defer idxFile.Close()

	inHead := true
	err = idx.WalkIndexFile(idxFile, 0, func(_ types.NeedleId, offset types.Offset, size types.Size) error {
		if inHead {
			if offset.IsZero() && size.IsTombstone() {
				clobbered++
				return nil
			}
			inHead = false
			if clobbered == 0 {
				return errStopIdxWalk
			}
		}
		if offset.ToActualOffset() == firstNeedleOffset {
			headIndexed = true
			return errStopIdxWalk
		}
		return nil
	})
	if errors.Is(err, errStopIdxWalk) {
		err = nil
	}
	return clobbered, headIndexed, err
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

func appendIdxRows(idxFileName string, rows []byte) error {
	idxFile, err := os.OpenFile(idxFileName, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer idxFile.Close()
	if _, err = idxFile.Write(rows); err != nil {
		return err
	}
	return idxFile.Sync()
}
