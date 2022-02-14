package mount

import (
	"github.com/hanwen/go-fuse/v2/fuse"
)

func setBlksize(out *fuse.Attr, size uint32) {
	out.Blksize = size
}
