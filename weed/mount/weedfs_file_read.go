package mount

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/hanwen/go-fuse/v2/fuse"
	"io"
)

/**
 * Read data
 *
 * Read should send exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the file
 * has been opened in 'direct_io' mode, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_iov
 *   fuse_reply_data
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size number of bytes to read
 * @param off offset to read from
 * @param fi file information
 */
func (wfs *WFS) Read(cancel <-chan struct{}, in *fuse.ReadIn, buff []byte) (fuse.ReadResult, fuse.Status) {
	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		return nil, fuse.ENOENT
	}

	offset := int64(in.Offset)
	fh.lockForRead(offset, len(buff))
	defer fh.unlockForRead(offset, len(buff))

	totalRead, err := fh.readFromChunks(buff, offset)
	if err == nil || err == io.EOF {
		maxStop := fh.readFromDirtyPages(buff, offset)
		totalRead = max(maxStop-offset, totalRead)
	}
	if err == io.EOF {
		err = nil
	}

	if err != nil {
		glog.Warningf("file handle read %s %d: %v", fh.FullPath(), totalRead, err)
		return nil, fuse.EIO
	}

	return fuse.ReadResultData(buff[:totalRead]), fuse.OK
}
