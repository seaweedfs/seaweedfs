package needle

import (
	"bytes"
	"fmt"

	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func writeNeedleByVersion(version Version, n *Needle, offset uint64, bytesBuffer *bytes.Buffer) (size Size, actualSize int64, err error) {
	// Switch logic moved from needle_write.go
	switch version {
	case Version1:
		size, actualSize, err = writeNeedleV1(n, offset, bytesBuffer)
	case Version2:
		size, actualSize, err = writeNeedleV2(n, offset, bytesBuffer)
	case Version3:
		size, actualSize, err = writeNeedleV3(n, offset, bytesBuffer)
	default:
		err = fmt.Errorf("unsupported version: %d", version)
	}
	return
}
