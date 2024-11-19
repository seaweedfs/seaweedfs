package storage

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

const PagedReadLimit = 1024 * 1024

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) readNeedle(n *needle.Needle, readOption *ReadOption, onReadSizeFn func(size Size)) (count int, err error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	nv, ok := v.nm.Get(n.Id)
	if !ok || nv.Offset.IsZero() {
		return -1, ErrorNotFound
	}
	readSize := nv.Size
	if readSize.IsDeleted() {
		if readOption != nil && readOption.ReadDeleted && readSize != TombstoneFileSize {
			glog.V(3).Infof("reading deleted %s", n.String())
			readSize = -readSize
		} else {
			return -1, ErrorDeleted
		}
	}
	if readSize == 0 {
		return 0, nil
	}
	if onReadSizeFn != nil {
		onReadSizeFn(readSize)
	}
	if readOption != nil && readOption.AttemptMetaOnly && readSize > PagedReadLimit {
		readOption.VolumeRevision = v.SuperBlock.CompactionRevision
		err = n.ReadNeedleMeta(v.DataBackend, nv.Offset.ToActualOffset(), readSize, v.Version())
		if err == needle.ErrorSizeMismatch && OffsetSize == 4 {
			readOption.IsOutOfRange = true
			err = n.ReadNeedleMeta(v.DataBackend, nv.Offset.ToActualOffset()+int64(MaxPossibleVolumeSize), readSize, v.Version())
		}
		if err != nil {
			return 0, err
		}
		if !n.IsCompressed() && !n.IsChunkedManifest() {
			readOption.IsMetaOnly = true
		}
	}
	if readOption == nil || !readOption.IsMetaOnly {
		err = n.ReadData(v.DataBackend, nv.Offset.ToActualOffset(), readSize, v.Version())
		v.checkReadWriteError(err)
		if err != nil {
			return 0, err
		}
	}
	count = int(n.DataSize)
	if !n.HasTtl() {
		return
	}
	ttlMinutes := n.Ttl.Minutes()
	if ttlMinutes == 0 {
		return
	}
	if !n.HasLastModifiedDate() {
		return
	}
	if time.Now().Before(time.Unix(0, int64(n.AppendAtNs)).Add(time.Duration(ttlMinutes) * time.Minute)) {
		return
	}
	return -1, ErrorNotFound
}

// read needle at a specific offset
func (v *Volume) readNeedleMetaAt(n *needle.Needle, offset int64, size int32) (err error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()
	// read deleted needle meta data
	if size < 0 {
		size = 0
	}
	err = n.ReadNeedleMeta(v.DataBackend, offset, Size(size), v.Version())
	if err == needle.ErrorSizeMismatch && OffsetSize == 4 {
		err = n.ReadNeedleMeta(v.DataBackend, offset+int64(MaxPossibleVolumeSize), Size(size), v.Version())
	}
	if err != nil {
		return err
	}
	return nil
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) readNeedleDataInto(n *needle.Needle, readOption *ReadOption, writer io.Writer, offset int64, size int64) (err error) {

	if !readOption.HasSlowRead {
		v.dataFileAccessLock.RLock()
		defer v.dataFileAccessLock.RUnlock()
	}

	if readOption.HasSlowRead {
		v.dataFileAccessLock.RLock()
	}
	nv, ok := v.nm.Get(n.Id)
	if readOption.HasSlowRead {
		v.dataFileAccessLock.RUnlock()
	}

	if !ok || nv.Offset.IsZero() {
		return ErrorNotFound
	}
	readSize := nv.Size
	if readSize.IsDeleted() {
		if readOption != nil && readOption.ReadDeleted && readSize != TombstoneFileSize {
			glog.V(3).Infof("reading deleted %s", n.String())
			readSize = -readSize
		} else {
			return ErrorDeleted
		}
	}
	if readSize == 0 {
		return nil
	}

	actualOffset := nv.Offset.ToActualOffset()
	if readOption.IsOutOfRange {
		actualOffset += int64(MaxPossibleVolumeSize)
	}

	buf := mem.Allocate(min(readOption.ReadBufferSize, int(size)))
	defer mem.Free(buf)

	// read needle data
	crc := needle.CRC(0)
	for x := offset; x < offset+size; x += int64(len(buf)) {

		if readOption.HasSlowRead {
			v.dataFileAccessLock.RLock()
		}
		// possibly re-read needle offset if volume is compacted
		if readOption.VolumeRevision != v.SuperBlock.CompactionRevision {
			// the volume is compacted
			nv, ok = v.nm.Get(n.Id)
			if !ok || nv.Offset.IsZero() {
				if readOption.HasSlowRead {
					v.dataFileAccessLock.RUnlock()
				}
				return ErrorNotFound
			}
			actualOffset = nv.Offset.ToActualOffset()
			readOption.VolumeRevision = v.SuperBlock.CompactionRevision
		}
		count, err := n.ReadNeedleData(v.DataBackend, actualOffset, buf, x)
		if readOption.HasSlowRead {
			v.dataFileAccessLock.RUnlock()
		}

		toWrite := min(count, int(offset+size-x))
		if toWrite > 0 {
			crc = crc.Update(buf[0:toWrite])
			// the crc.Value() function is to be deprecated. this double checking is for backward compatibility
			// with seaweed version using crc.Value() instead of uint32(crc), which appears in commit 056c480eb
			// and switch appeared in version 3.09.
			if offset == 0 && size == int64(n.DataSize) && int64(count) == size && (n.Checksum != crc && uint32(n.Checksum) != crc.Value()) {
				// This check works only if the buffer is big enough to hold the whole needle data
				// and we ask for all needle data.
				// Otherwise we cannot check the validity of partially aquired data.
				stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorCRC).Inc()
				return fmt.Errorf("ReadNeedleData checksum %v expected %v for Needle: %v,%v", crc, n.Checksum, v.Id, n)
			}
			if _, err = writer.Write(buf[0:toWrite]); err != nil {
				return fmt.Errorf("ReadNeedleData write: %v", err)
			}
		}
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			return fmt.Errorf("ReadNeedleData: %v", err)
		}
		if count <= 0 {
			break
		}
	}
	if offset == 0 && size == int64(n.DataSize) && (n.Checksum != crc && uint32(n.Checksum) != crc.Value()) {
		// the crc.Value() function is to be deprecated. this double checking is for backward compatibility
		// with seaweed version using crc.Value() instead of uint32(crc), which appears in commit 056c480eb
		// and switch appeared in version 3.09.
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorCRC).Inc()
		return fmt.Errorf("ReadNeedleData checksum %v expected %v for Needle: %v,%v", crc, n.Checksum, v.Id, n)
	}
	return nil

}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) ReadNeedleBlob(offset int64, size Size) ([]byte, error) {
	v.dataFileAccessLock.RLock()
	defer v.dataFileAccessLock.RUnlock()

	return needle.ReadNeedleBlob(v.DataBackend, offset, size, v.Version())
}

type VolumeFileScanner interface {
	VisitSuperBlock(super_block.SuperBlock) error
	ReadNeedleBody() bool
	VisitNeedle(n *needle.Needle, offset int64, needleHeader, needleBody []byte) error
}

func ScanVolumeFile(dirname string, collection string, id needle.VolumeId,
	needleMapKind NeedleMapKind,
	volumeFileScanner VolumeFileScanner) (err error) {
	var v *Volume
	if v, err = loadVolumeWithoutIndex(dirname, collection, id, needleMapKind); err != nil {
		return fmt.Errorf("failed to load volume %d: %v", id, err)
	}
	if err = volumeFileScanner.VisitSuperBlock(v.SuperBlock); err != nil {
		return fmt.Errorf("failed to process volume %d super block: %v", id, err)
	}
	defer v.Close()

	version := v.Version()

	offset := int64(v.SuperBlock.BlockSize())

	return ScanVolumeFileFrom(version, v.DataBackend, offset, volumeFileScanner)
}

func ScanVolumeFileFrom(version needle.Version, datBackend backend.BackendStorageFile, offset int64, volumeFileScanner VolumeFileScanner) (err error) {
	n, nh, rest, e := needle.ReadNeedleHeader(datBackend, version, offset)
	if e != nil {
		if e == io.EOF {
			return nil
		}
		return fmt.Errorf("cannot read %s at offset %d: %v", datBackend.Name(), offset, e)
	}
	for n != nil {
		var needleBody []byte
		if volumeFileScanner.ReadNeedleBody() {
			// println("needle", n.Id.String(), "offset", offset, "size", n.Size, "rest", rest)
			if needleBody, err = n.ReadNeedleBody(datBackend, version, offset+NeedleHeaderSize, rest); err != nil {
				glog.V(0).Infof("cannot read needle head [%d, %d) body [%d, %d) body length %d: %v", offset, offset+NeedleHeaderSize, offset+NeedleHeaderSize, offset+NeedleHeaderSize+rest, rest, err)
				// err = fmt.Errorf("cannot read needle body: %v", err)
				// return
			}
		}
		err := volumeFileScanner.VisitNeedle(n, offset, nh, needleBody)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			glog.V(0).Infof("visit needle error: %v", err)
			return fmt.Errorf("visit needle error: %v", err)
		}
		offset += NeedleHeaderSize + rest
		glog.V(4).Infof("==> new entry offset %d", offset)
		if n, nh, rest, err = needle.ReadNeedleHeader(datBackend, version, offset); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("cannot read needle header at offset %d: %v", offset, err)
		}
		glog.V(4).Infof("new entry needle size:%d rest:%d", n.Size, rest)
	}
	return nil
}
