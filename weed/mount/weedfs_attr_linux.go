package mount

import (
	"github.com/seaweedfs/go-fuse/v2/fuse"
)

func setBlksize(out *fuse.Attr, size uint32) {
	out.Blksize = size
}
