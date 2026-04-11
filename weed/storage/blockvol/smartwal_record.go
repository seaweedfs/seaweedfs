package blockvol

import (
	"encoding/binary"
	"hash/crc32"
)

// SmartWAL: metadata-only WAL where data lives in the extent.
// Each record is 32 bytes fixed. No data payload.
//
// The WAL + extent together form a logical LSN-ordered storage:
//   - WAL provides ordering, crash-recovery metadata, and CRC verification
//   - Extent provides authoritative data storage
//
// On crash recovery, WAL records are scanned and extent data is verified
// via CRC. Records with mismatched CRCs are skipped (torn/unflushed writes).

const (
	SmartWALRecordSize = 32

	// Record flags
	SmartFlagWrite   uint8 = 0x01
	SmartFlagTrim    uint8 = 0x02
	SmartFlagBarrier uint8 = 0x04

	// Magic byte for record validation (distinguishes valid records from zeros)
	smartRecordMagic uint8 = 0xAC
)

// SmartWALRecord is the metadata-only WAL entry.
// Wire format: [1B magic][1B flags][2B pad][4B lba][8B lsn][8B epoch][4B dataCRC][4B recCRC] = 32 bytes
type SmartWALRecord struct {
	LSN       uint64
	Epoch     uint64
	LBA       uint32
	Flags     uint8
	DataCRC32 uint32 // crc32c of the 4KB data block in extent
}

// EncodeSmartWALRecord serializes a record into exactly 32 bytes.
// Layout: [magic:1][flags:1][pad:2][lba:4][lsn:8][epoch:8][dataCRC:4][recCRC:4]
// The recCRC covers bytes 0..27 (everything except the recCRC field itself).
func EncodeSmartWALRecord(rec SmartWALRecord) [SmartWALRecordSize]byte {
	var buf [SmartWALRecordSize]byte
	buf[0] = smartRecordMagic
	buf[1] = rec.Flags
	// buf[2..3] = padding (zero)
	binary.LittleEndian.PutUint32(buf[4:8], rec.LBA)
	binary.LittleEndian.PutUint64(buf[8:16], rec.LSN)
	binary.LittleEndian.PutUint64(buf[16:24], rec.Epoch)
	binary.LittleEndian.PutUint32(buf[24:28], rec.DataCRC32)
	// Record CRC covers bytes 0..27
	recCRC := crc32.ChecksumIEEE(buf[:28])
	binary.LittleEndian.PutUint32(buf[28:32], recCRC)
	return buf
}

// DecodeSmartWALRecord deserializes a 32-byte record.
// Returns (record, true) if the record is valid (magic + CRC check).
// Returns (zero, false) if the record is invalid, zeroed, or torn.
func DecodeSmartWALRecord(buf []byte) (SmartWALRecord, bool) {
	if len(buf) < SmartWALRecordSize {
		return SmartWALRecord{}, false
	}
	// Check magic
	if buf[0] != smartRecordMagic {
		return SmartWALRecord{}, false
	}
	// Verify record CRC
	expectedCRC := binary.LittleEndian.Uint32(buf[28:32])
	actualCRC := crc32.ChecksumIEEE(buf[:28])
	if expectedCRC != actualCRC {
		return SmartWALRecord{}, false
	}
	return SmartWALRecord{
		Flags:     buf[1],
		LBA:       binary.LittleEndian.Uint32(buf[4:8]),
		LSN:       binary.LittleEndian.Uint64(buf[8:16]),
		Epoch:     binary.LittleEndian.Uint64(buf[16:24]),
		DataCRC32: binary.LittleEndian.Uint32(buf[24:28]),
	}, true
}
