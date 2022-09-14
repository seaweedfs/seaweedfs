package mount

import (
	"context"
	"io"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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

	fh.orderedMutex.Acquire(context.Background(), 1)
	defer fh.orderedMutex.Release(1)

	offset := int64(in.Offset)
	totalRead, err := readDataByFileHandle(buff, fh, offset)
	if err != nil {
		glog.Warningf("file handle read %s %d: %v", fh.FullPath(), totalRead, err)
		return nil, fuse.EIO
	}

	return fuse.ReadResultData(buff[:totalRead]), fuse.OK
}

func readDataByFileHandle(buff []byte, fhIn *FileHandle, offset int64) (int64, error) {
	// read data from source file
	size := len(buff)
	fhIn.lockForRead(offset, size)
	defer fhIn.unlockForRead(offset, size)

	n, err := fhIn.readFromChunks(buff, offset)
	if err == nil || err == io.EOF {
		maxStop := fhIn.readFromDirtyPages(buff, offset)
		n = max(maxStop-offset, n)
	}
	if err == io.EOF {
		err = nil
	}
	return n, err
}
