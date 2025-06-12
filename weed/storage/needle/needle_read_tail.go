package needle

import (
	"errors"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (n *Needle) readNeedleTail(needleBody []byte, version Version) error {

	// for all versions, we need to read the checksum
	if len(n.Data) > 0 {
		expectedChecksum := CRC(util.BytesToUint32(needleBody[0:NeedleChecksumSize]))
		dataChecksum := NewCRC(n.Data)
		if expectedChecksum != dataChecksum {
			// the crc.Value() function is to be deprecated. this double checking is for backward compatibility
			// with seaweed version using crc.Value() instead of uint32(crc), which appears in commit 056c480eb
			// and switch appeared in version 3.09.
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorCRC).Inc()
			return errors.New("CRC error! Data On Disk Corrupted")
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
