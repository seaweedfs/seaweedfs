package storage

import (
	"encoding/binary"

	"github.com/cespare/xxhash/v2"
)

// ReportHash digests everything a volume server reports about a volume, so the
// two ends of a heartbeat can agree on whether the master's copy is current
// without shipping the volume list.
//
// It must cover every field of VolumeInformationMessage: a change the hash
// misses is a change the master would never be told about. Volume servers hash
// the message they are about to send, masters hash what they already hold, and
// the two match only when the master is up to date.
func (vi VolumeInfo) ReportHash() uint64 {
	var buf [57]byte
	binary.LittleEndian.PutUint32(buf[0:], uint32(vi.Id))
	binary.LittleEndian.PutUint64(buf[4:], vi.Size)
	binary.LittleEndian.PutUint64(buf[12:], uint64(vi.FileCount))
	binary.LittleEndian.PutUint64(buf[20:], uint64(vi.DeleteCount))
	binary.LittleEndian.PutUint64(buf[28:], vi.DeletedByteCount)
	binary.LittleEndian.PutUint32(buf[36:], uint32(vi.ReplicaPlacement.Byte()))
	binary.LittleEndian.PutUint32(buf[40:], uint32(vi.Version))
	binary.LittleEndian.PutUint32(buf[44:], vi.Ttl.ToUint32())
	binary.LittleEndian.PutUint32(buf[48:], vi.CompactRevision)
	binary.LittleEndian.PutUint32(buf[52:], vi.DiskId)
	if vi.ReadOnly {
		buf[56] = 1
	}
	h := xxhash.Sum64(buf[:])

	var modified [8]byte
	binary.LittleEndian.PutUint64(modified[:], uint64(vi.ModifiedAtSecond))
	h = foldReportHash(h, xxhash.Sum64(modified[:]))
	h = foldReportHash(h, xxhash.Sum64String(vi.Collection))
	h = foldReportHash(h, xxhash.Sum64String(vi.DiskType))
	h = foldReportHash(h, xxhash.Sum64String(vi.RemoteStorageName))
	return h
}

// foldReportHash combines two hashes order-dependently, so swapping two string
// fields is not invisible.
func foldReportHash(h, x uint64) uint64 {
	h ^= x
	h *= 0x9E3779B97F4A7C15
	return h ^ (h >> 29)
}
