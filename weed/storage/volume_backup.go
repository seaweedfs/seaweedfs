package storage

import (
	"context"
	"fmt"
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

	err = operation.WithVolumeServerClient(false, volumeServer, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		stream, err := client.VolumeIncrementalCopy(context.Background(), &volume_server_pb.VolumeIncrementalCopyRequest{
			VolumeId:         uint32(v.Id),
			SinceNs:          cursor.appendAtNs,
			EntryCount:       cursor.entryCount,
			LastNeedleKey:    uint64(cursor.key),
			LastNeedleCookie: cursor.cookie,
			LastNeedleSize:   int32(cursor.size),
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
			n, writeErr := v.DataBackend.WriteAt(resp.FileContent, writeOffset)
			if writeErr != nil {
				return writeErr
			}
			writeOffset += int64(n)
		}

		return nil

	})

	if err != nil {
		return err
	}

	// add to needle map
	return ScanVolumeFileFrom(v.Version(), v.DataBackend, int64(startFromOffset), &VolumeFileScanner4GenIdx{v: v})

}

// backupCursor identifies the last record of the local copy: its index
// position and the needle itself. A replica can verify the same record at the
// same position and resume from there, which stays correct when a different
// replica answers, because appendAtNs stamps differ across replicas.
type backupCursor struct {
	entryCount uint64
	key        NeedleId
	size       Size
	cookie     uint32
	appendAtNs uint64
}

func (v *Volume) backupCursor() (backupCursor, error) {
	key, offset, size, entryCount, err := v.locateLastAppendEntry()
	if err != nil || offset.IsZero() {
		return backupCursor{entryCount: entryCount}, err
	}
	n, err := v.readNeedleAt(offset)
	if err != nil {
		return backupCursor{}, err
	}
	return backupCursor{
		entryCount: entryCount,
		key:        key,
		size:       size,
		cookie:     uint32(n.Cookie),
		appendAtNs: n.AppendAtNs,
	}, nil
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
// entry in key, size and cookie; the copy then continues from the replica's
// entry at position entryCount, so nothing is skipped or appended twice.
func (v *Volume) ResumeByLastEntry(entryCount uint64, key NeedleId, cookie uint32, size Size) (offset Offset, upToDate bool, err error) {

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
	n, e := v.readNeedleAt(lastOffset)
	if e != nil {
		err = e
		return
	}
	if uint32(n.Cookie) != cookie {
		err = ErrIncrementalCopyDiverged
		return
	}

	if serverCount == entryCount {
		upToDate = true
		return
	}
	offset, err = v.readOffsetFromIndex(int64(entryCount))
	if err == nil && offset.ToActualOffset() <= 0 {
		err = ErrIncrementalCopyDiverged
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
