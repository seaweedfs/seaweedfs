package mount

import "github.com/hanwen/go-fuse/v2/fuse"

// https://github.com/libfuse/libfuse/blob/48ae2e72b39b6a31cb2194f6f11786b7ca06aac6/include/fuse.h#L778

/**
 * Allocates space for an open file
 *
 * This function ensures that required space is allocated for specified
 * file.  If this function returns success then any subsequent write
 * request to specified range is guaranteed not to fail because of lack
 * of space on the file system media.
 */
func (wfs *WFS) Fallocate(cancel <-chan struct{}, in *fuse.FallocateIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (wfs *WFS) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (wfs *WFS) SetLk(cancel <-chan struct{}, in *fuse.LkIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (wfs *WFS) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) (code fuse.Status) {
	return fuse.ENOSYS
}

/**
 * Check file access permissions
 *
 * This will be called for the access() system call.  If the
 * 'default_permissions' mount option is given, this method is not
 * called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 */
func (wfs *WFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) (code fuse.Status) {
	return fuse.ENOSYS
}
