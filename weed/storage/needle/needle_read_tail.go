package needle

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (n *Needle) readNeedleTail(needleBody []byte, version Version) error {
	// for all versions, we need to read the checksum
	if len(n.Data) > 0 {
		expectedChecksum := CRC(util.BytesToUint32(needleBody[0:NeedleChecksumSize]))
		dataChecksum := NewCRC(n.Data)
		if expectedChecksum != dataChecksum && uint32(expectedChecksum) != dataChecksum.Value() {
			// crc.Value() is the deprecated legacy transform used before commit 056c480eb
			// (volume server <3.09). Accept either encoding so .dat files written by older
			// versions still verify after an upgrade.
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorCRC).Inc()
			return fmt.Errorf("invalid CRC for needle %v (got %08x, want %08x), data on disk corrupted: %w", n.Id, dataChecksum, expectedChecksum, ErrorCorrupted)
		}
		n.Checksum = dataChecksum
	} else {
		// when data is skipped from reading, just read the checksum
		n.Checksum = CRC(util.BytesToUint32(needleBody[0:NeedleChecksumSize]))
	}

	if version == Version3 {
		tsOffset := NeedleChecksumSize
		n.AppendAtNs = util.BytesToUint64(needleBody[tsOffset : tsOffset+TimestampSize])
	}
	return nil
}

func PaddingLength(needleSize Size, version Version) Size {
	if version == Version3 {
		// this is same value as version2, but just listed here for clarity
		return NeedlePaddingSize - ((NeedleHeaderSize + needleSize + NeedleChecksumSize + TimestampSize) % NeedlePaddingSize)
	}
	return NeedlePaddingSize - ((NeedleHeaderSize + needleSize + NeedleChecksumSize) % NeedlePaddingSize)
}

func NeedleBodyLength(needleSize Size, version Version) int64 {
	if version == Version3 {
		return int64(needleSize) + NeedleChecksumSize + TimestampSize + int64(PaddingLength(needleSize, version))
	}
	return int64(needleSize) + NeedleChecksumSize + int64(PaddingLength(needleSize, version))
}
