package storage

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/pb"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/idx"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (v *Volume) GetVolumeSyncStatus() *volume_server_pb.VolumeSyncStatusResponse {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	var syncStatus = &volume_server_pb.VolumeSyncStatusResponse{}
	if datSize, _, err := v.DataBackend.GetStat(); err == nil {
		syncStatus.TailOffset = uint64(datSize)
	}
	syncStatus.VolumeId = uint32(v.Id)
	syncStatus.Collection = v.Collection
	syncStatus.IdxFileSize = v.nm.IndexFileSize()
	syncStatus.CompactRevision = uint32(v.SuperBlock.CompactionRevision)
	syncStatus.Ttl = v.SuperBlock.Ttl.String()
	syncStatus.Replication = v.SuperBlock.ReplicaPlacement.String()
	syncStatus.Version = uint32(v.SuperBlock.Version)
	return syncStatus
}

// The volume sync with a master volume via 2 steps:
// 1. The slave checks master side to find subscription checkpoint
//	  to setup the replication.
// 2. The slave receives the updates from master

/*
Assume the slave volume needs to follow the master volume.

The master volume could be compacted, and could be many files ahead of
slave volume.

Step 0: // implemented in command/backup.go, to avoid dat file size overflow.
0.1 If slave compact version is less than the master, do a local compaction, and set
local compact version the same as the master.
0.2 If the slave size is still bigger than the master, discard local copy and do a full copy.

Step 1:
The slave volume ask the master by the last modification time t.
The master do a binary search in volume (use .idx as an array, and check the appendAtNs in .dat file),
to find the first entry with appendAtNs > t.

Step 2:
The master send content bytes to the slave. The bytes are not chunked by needle.

Step 3:
The slave generate the needle map for the new bytes. (This may be optimized to incrementally
update needle map when receiving new .dat bytes. But seems not necessary now.)

*/

func (v *Volume) IncrementalBackup(volumeServer pb.ServerAddress, grpcDialOption grpc.DialOption) error {

	if err := v.UnavailableError(); err != nil {
		return err
	}

	startFromOffset, _, _ := v.FileStat()
	cursor, err := v.backupCursor()
	if err != nil {
		return err
	}

	writeOffset := int64(startFromOffset)
	indexOffset := int64(cursor.entryCount) * NeedleMapEntrySize
	// only the in-memory map can reload rows it just wrote to the index
	// file; other maps fall back to regenerating the index from .dat
	cnm, canReplayIndex := v.nm.(*NeedleMap)
	receivedIndex := false

	err = operation.WithVolumeServerClient(false, volumeServer, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		stream, err := client.VolumeIncrementalCopy(context.Background(), &volume_server_pb.VolumeIncrementalCopyRequest{
			VolumeId:         uint32(v.Id),
			SinceNs:          cursor.appendAtNs,
			EntryCount:       cursor.entryCount,
			LastNeedleKey:    uint64(cursor.key),
			LastNeedleCookie: cursor.cookie,
			LastNeedleSize:   int32(cursor.size),
			EntriesHash:      cursor.entriesHash,
		})
		if err != nil {
			return err
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				} else {
					return recvErr
				}
			}

			if err := v.UnavailableError(); err != nil {
				return err
			}
			if len(resp.IdxContent) > 0 {
				receivedIndex = true
				if !canReplayIndex {
					continue
				}
				n, writeErr := cnm.IndexFile().WriteAt(resp.IdxContent, indexOffset)
				if writeErr != nil {
					return writeErr
				}
				indexOffset += int64(n)
				continue
			}
			n, writeErr := v.DataBackend.WriteAt(resp.FileContent, writeOffset)
			if writeErr != nil {
				return writeErr
			}
			writeOffset += int64(n)
		}

		return nil

	})

	if err != nil {
		// Roll the files back to the pre-copy boundary: leftover bytes could
		// split a row or be mistaken for applied ones on the next run.
		if receivedIndex && canReplayIndex {
			_ = cnm.truncateIndex(int64(cursor.entryCount) * NeedleMapEntrySize)
		}
		if writeOffset > int64(startFromOffset) {
			_ = v.DataBackend.Truncate(int64(startFromOffset))
		}
		return err
	}

	if receivedIndex && canReplayIndex {
		baseOffset := int64(cursor.entryCount) * NeedleMapEntrySize
		if err := cnm.truncateIndex(indexOffset); err != nil {
			_ = cnm.truncateIndex(baseOffset)
			_ = v.DataBackend.Truncate(int64(startFromOffset))
			return err
		}
		// a failed replay is rolled back to the pre-copy boundary, so
		// snapshot the map entries the received tail is about to touch
		var replayKeys []NeedleId
		seen := make(map[NeedleId]bool)
		_ = idx.WalkIndexFile(cnm.IndexFile(), uint64(cursor.entryCount), func(key NeedleId, _ Offset, _ Size) error {
			if !seen[key] {
				seen[key] = true
				replayKeys = append(replayKeys, key)
			}
			return nil
		})
		type priorMapping struct {
			offset Offset
			size   Size
			exists bool
		}
		prior := make([]priorMapping, len(replayKeys))
		for i, key := range replayKeys {
			if nv, ok := cnm.Get(key); ok {
				prior[i] = priorMapping{offset: nv.Offset, size: nv.Size, exists: true}
			}
		}
		metrics := cnm.snapshotBatchMetrics()

		if err := cnm.DoOffsetLoading(v, cnm.IndexFile(), uint64(cursor.entryCount)); err != nil {
			// a half-replayed tail cannot be trusted as the next cursor
			_ = cnm.truncateIndex(baseOffset)
			_ = v.DataBackend.Truncate(int64(startFromOffset))
			for i, key := range replayKeys {
				if prior[i].exists {
					_ = cnm.restoreMapping(key, prior[i].offset, prior[i].size)
				} else {
					_ = cnm.removeMapping(key)
				}
			}
			cnm.restoreBatchMetrics(metrics)
			return err
		}
		return nil
	}

	// add to needle map
	return ScanVolumeFileFrom(v.Version(), v.DataBackend, int64(startFromOffset), &VolumeFileScanner4GenIdx{v: v})

}

// backupCursor identifies the last record of the local copy: its index
// position and the needle itself. A replica can verify the same record at the
// same position and resume from there, which stays correct when a different
// replica answers, because appendAtNs stamps differ across replicas.
type backupCursor struct {
	entryCount  uint64
	key         NeedleId
	size        Size
	cookie      uint32
	appendAtNs  uint64
	entriesHash uint64
}

func (v *Volume) backupCursor() (backupCursor, error) {
	key, offset, size, entryCount, err := v.locateLastAppendEntry()
	if err != nil {
		return backupCursor{entryCount: entryCount}, err
	}
	entriesHash, err := v.hashIndexEntries(entryCount)
	if err != nil {
		return backupCursor{}, err
	}
	if offset.IsZero() {
		// an index-only tombstone has no needle to read; key and size carry it
		return backupCursor{entryCount: entryCount, key: key, size: size, entriesHash: entriesHash}, nil
	}
	n, err := v.readNeedleAt(offset)
	if err != nil {
		return backupCursor{}, err
	}
	return backupCursor{
		entryCount:  entryCount,
		key:         key,
		size:        size,
		cookie:      uint32(n.Cookie),
		appendAtNs:  n.AppendAtNs,
		entriesHash: entriesHash,
	}, nil
}

// hashIndexEntries folds the (key, size) of the first entryCount index entries
// into one FNV-1a checksum. The offset field is skipped because the same
// record sits at different offsets on different replicas.
func (v *Volume) hashIndexEntries(entryCount uint64) (uint64, error) {
	indexFile, err := os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("cannot read %s: %v", v.FileName(".idx"), err)
	}
	defer indexFile.Close()

	h := fnv.New64a()
	buf := make([]byte, 1<<20)
	remaining := int64(entryCount) * NeedleMapEntrySize
	for remaining > 0 {
		want := int64(len(buf))
		if remaining < want {
			want = remaining
		}
		want -= want % int64(NeedleMapEntrySize)
		read, err := io.ReadFull(indexFile, buf[:want])
		if err != nil {
			return 0, fmt.Errorf("file %s read error: %v", indexFile.Name(), err)
		}
		for i := 0; i+NeedleMapEntrySize <= read; i += NeedleMapEntrySize {
			h.Write(buf[i : i+NeedleIdSize])
			h.Write(buf[i+NeedleIdSize+OffsetSize : i+NeedleMapEntrySize])
		}
		remaining -= int64(read)
	}
	return h.Sum64(), nil
}

func (v *Volume) locateLastAppendEntry() (key NeedleId, offset Offset, size Size, entryCount uint64, err error) {
	indexFile, e := os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644)
	if e != nil {
		err = fmt.Errorf("cannot read %s: %v", v.FileName(".idx"), e)
		return
	}
	defer indexFile.Close()

	fi, e := indexFile.Stat()
	if e != nil {
		err = fmt.Errorf("file %s stat error: %v", indexFile.Name(), e)
		return
	}
	fileSize := fi.Size()
	if fileSize%NeedleMapEntrySize != 0 {
		err = fmt.Errorf("unexpected file %s size: %d", indexFile.Name(), fileSize)
		return
	}
	entryCount = uint64(fileSize / NeedleMapEntrySize)
	if fileSize == 0 {
		return
	}

	bytes := make([]byte, NeedleMapEntrySize)
	n, e := indexFile.ReadAt(bytes, fileSize-NeedleMapEntrySize)
	if n != NeedleMapEntrySize {
		err = fmt.Errorf("file %s read error: %v", indexFile.Name(), e)
		return
	}
	key, offset, size = idx.IdxFileEntry(bytes)
	return
}

func (v *Volume) readNeedleAt(offset Offset) (*needle.Needle, error) {
	n, _, bodyLength, err := needle.ReadNeedleHeader(v.DataBackend, v.SuperBlock.Version, offset.ToActualOffset())
	if err != nil {
		return nil, fmt.Errorf("ReadNeedleHeader %s [%d,%d): %v", v.DataBackend.Name(), offset.ToActualOffset(), offset.ToActualOffset()+NeedleHeaderSize, err)
	}
	_, err = n.ReadNeedleBody(v.DataBackend, v.SuperBlock.Version, offset.ToActualOffset()+NeedleHeaderSize, bodyLength)
	if err != nil {
		return nil, fmt.Errorf("ReadNeedleBody offset %d, bodyLength %d: %v", offset.ToActualOffset(), bodyLength, err)
	}
	return n, nil
}

func (v *Volume) readAppendAtNs(offset Offset) (uint64, error) {
	n, err := v.readNeedleAt(offset)
	if err != nil {
		return 0, err
	}
	return n.AppendAtNs, nil
}

// ErrIncrementalCopyDiverged means the local copy is not a prefix of this
// replica's records, so an incremental copy cannot resume safely and the
// backup should be rebuilt from scratch.
var ErrIncrementalCopyDiverged = fmt.Errorf("local copy diverged from the replica")

// ResumeByLastEntry resumes an incremental copy by index position instead of
// by appendAtNs, whose stamps differ across replicas for the same needle.
// The replica's entry at position entryCount-1 must match the backup's last
// entry in key and size — and cookie, unless it is an index-only tombstone
// (a remote-tier delete), which has no .dat record to read a cookie from —
// and its first entryCount entries must hash the same — a boundary match
// alone is not proof, since replicated writes can land in a different order
// on this replica. The copy then continues from the replica's next entry
// with a .dat record; index-only rows ride the index stream, so nothing is
// skipped or appended twice. indexBound caps the scan to the index size the
// caller captured, so rows appended after that point stay out of this copy.
func (v *Volume) ResumeByLastEntry(entryCount uint64, key NeedleId, cookie uint32, size Size, entriesHash uint64, indexBound uint64) (offset Offset, upToDate bool, err error) {

	fileSize := int64(v.IndexFileSize())
	if fileSize%NeedleMapEntrySize != 0 {
		err = fmt.Errorf("unexpected file %s.idx size: %d", v.IndexFileName(), fileSize)
		return
	}
	serverCount := uint64(fileSize / NeedleMapEntrySize)
	if serverCount < entryCount {
		err = ErrIncrementalCopyDiverged
		return
	}

	lastKey, lastOffset, lastSize, e := v.readIndexEntry(int64(entryCount) - 1)
	if e != nil {
		err = e
		return
	}
	if lastKey != key || lastSize != size {
		err = ErrIncrementalCopyDiverged
		return
	}
	if !lastOffset.IsZero() {
		var n *needle.Needle
		n, e = v.readNeedleAt(lastOffset)
		if e != nil {
			err = e
			return
		}
		if uint32(n.Cookie) != cookie {
			err = ErrIncrementalCopyDiverged
			return
		}
	}

	prefixHash, e := v.hashIndexEntries(entryCount)
	if e != nil {
		err = e
		return
	}
	if prefixHash != entriesHash {
		err = ErrIncrementalCopyDiverged
		return
	}

	if serverCount == entryCount {
		upToDate = true
		return
	}
	scanEnd := int64(serverCount)
	if boundRows := int64(indexBound / uint64(NeedleMapEntrySize)); boundRows < scanEnd {
		scanEnd = boundRows
	}
	for i := int64(entryCount); i < scanEnd; i++ {
		if offset, err = v.readOffsetFromIndex(i); err != nil {
			return
		}
		if !offset.IsZero() {
			return
		}
	}
	return
}

// on server side
func (v *Volume) BinarySearchByAppendAtNs(sinceNs uint64) (offset Offset, isLast bool, err error) {

	fileSize := int64(v.IndexFileSize())
	if fileSize%NeedleMapEntrySize != 0 {
		err = fmt.Errorf("unexpected file %s.idx size: %d", v.IndexFileName(), fileSize)
		return
	}

	entryCount := fileSize / NeedleMapEntrySize
	l := int64(0)
	h := entryCount

	for l < h {

		m := (l + h) / 2

		if m == entryCount {
			return Offset{}, true, nil
		}

		// read the appendAtNs for entry m
		offset, err = v.readOffsetFromIndex(m)
		if err != nil {
			err = fmt.Errorf("read entry %d: %v", m, err)
			return
		}
		if offset.IsZero() {
			leftIndex, _, leftNs, leftErr := v.readLeftNs(m)
			if leftErr != nil {
				err = leftErr
				return
			}
			rightIndex, rightOffset, rightNs, rightErr := v.readRightNs(m, entryCount)
			if rightErr != nil {
				err = rightErr
				return
			}
			if rightNs <= sinceNs {
				l = rightIndex
				if l == entryCount {
					return Offset{}, true, nil
				} else {
					continue
				}
			}
			if sinceNs < leftNs {
				h = leftIndex + 1
				continue
			}
			return rightOffset, false, nil

		}
		if offset.IsZero() {
			return Offset{}, true, nil
		}

		mNs, nsReadErr := v.readAppendAtNs(offset)
		if nsReadErr != nil {
			err = fmt.Errorf("read entry %d offset %d: %v", m, offset.ToActualOffset(), nsReadErr)
			return
		}

		// move the boundary
		if mNs <= sinceNs {
			l = m + 1
		} else {
			h = m
		}

	}

	if l == entryCount {
		return Offset{}, true, nil
	}

	offset, err = v.readOffsetFromIndex(l)

	return offset, false, err

}

func (v *Volume) readRightNs(m, max int64) (index int64, offset Offset, ts uint64, err error) {
	index = m
	for offset.IsZero() {
		index++
		if index >= max {
			return
		}
		offset, err = v.readOffsetFromIndex(index)
		if err != nil {
			err = fmt.Errorf("read left entry at %d: %v", index, err)
			return
		}
	}
	if !offset.IsZero() {
		ts, err = v.readAppendAtNs(offset)
	}
	return
}

func (v *Volume) readLeftNs(m int64) (index int64, offset Offset, ts uint64, err error) {
	index = m
	for offset.IsZero() {
		index--
		if index < 0 {
			return
		}
		offset, err = v.readOffsetFromIndex(index)
		if err != nil {
			err = fmt.Errorf("read right entry at %d: %v", index, err)
			return
		}
	}
	if !offset.IsZero() {
		ts, err = v.readAppendAtNs(offset)
	}
	return
}

func (v *Volume) readIndexEntry(m int64) (key NeedleId, offset Offset, size Size, err error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	if v.nm == nil {
		return 0, Offset{}, 0, io.EOF
	}
	return v.nm.ReadIndexEntry(m)
}

// bytes is of size NeedleMapEntrySize
func (v *Volume) readOffsetFromIndex(m int64) (Offset, error) {
	_, offset, _, err := v.readIndexEntry(m)
	return offset, err
}

// DatSizeCoveredByIndex returns the .dat end of the last record the index
// rows before indexSize cover, or 0 when none does. Rows are not always in
// .dat order — rebuilds can write the .idx sorted by key — so the bound
// comes from the maximum offset: records do not overlap, so the highest
// offset names the highest end. Records appended after the index bound —
// including staged writes whose rows are not published yet — lie past it,
// so a copy must not stream them: they would reach the backup without their
// row and be appended again on the next run.
func (v *Volume) DatSizeCoveredByIndex(indexSize int64) int64 {
	indexFile, err := os.OpenFile(v.FileName(".idx"), os.O_RDONLY, 0644)
	if err != nil {
		return 0
	}
	defer indexFile.Close()

	boundRows := indexSize / NeedleMapEntrySize
	row := int64(0)
	maxActualOffset := int64(-1)
	err = idx.WalkIndexFile(indexFile, 0, func(_ NeedleId, offset Offset, _ Size) error {
		if row >= boundRows {
			return errStopIdxWalk
		}
		row++
		if !offset.IsZero() {
			if actualOffset := offset.ToActualOffset(); actualOffset > maxActualOffset {
				maxActualOffset = actualOffset
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopIdxWalk) {
		return 0
	}
	if maxActualOffset < 0 {
		return 0
	}
	_, _, bodyLength, err := needle.ReadNeedleHeader(v.DataBackend, v.SuperBlock.Version, maxActualOffset)
	if err != nil {
		return 0
	}
	return maxActualOffset + NeedleHeaderSize + bodyLength
}

// generate the volume idx
type VolumeFileScanner4GenIdx struct {
	v *Volume
}

func (scanner *VolumeFileScanner4GenIdx) VisitSuperBlock(superBlock super_block.SuperBlock) error {
	return nil

}
func (scanner *VolumeFileScanner4GenIdx) ReadNeedleBody() bool {
	return false
}

func (scanner *VolumeFileScanner4GenIdx) VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error {
	if n.Size > 0 && n.Size.IsValid() {
		return scanner.v.nm.Put(n.Id, ToOffset(offset), n.Size)
	}
	return scanner.v.nm.Delete(n.Id, ToOffset(offset))
}
