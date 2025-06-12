package mount

import (
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func (wfs *WFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (size uint32, code fuse.Status) {

	return 0, fuse.Status(syscall.ENOTSUP)
}

func (wfs *WFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {

	return fuse.Status(syscall.ENOTSUP)
}

func (wfs *WFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (n uint32, code fuse.Status) {

	return 0, fuse.Status(syscall.ENOTSUP)
}

func (wfs *WFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {

	return fuse.Status(syscall.ENOTSUP)
}
