package mount

import (
	"bytes"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

	fhActiveLock := fh.wfs.fhLockTable.AcquireLock("Read", fh.fh, util.SharedLock)
	defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	offset := int64(in.Offset)
	totalRead, err := readDataByFileHandle(buff, fh, offset)
	if err != nil {
		glog.Warningf("file handle read %s %d: %v", fh.FullPath(), totalRead, err)
		return nil, fuse.EIO
	}

	if IsDebugFileReadWrite {
		// print(".")
		mirrorData := make([]byte, totalRead)
		fh.mirrorFile.ReadAt(mirrorData, offset)
		if bytes.Compare(mirrorData, buff[:totalRead]) != 0 {

			againBuff := make([]byte, len(buff))
			againRead, _ := readDataByFileHandle(againBuff, fh, offset)
			againCorrect := bytes.Compare(mirrorData, againBuff[:againRead]) == 0
			againSame := bytes.Compare(buff[:totalRead], againBuff[:againRead]) == 0

			fmt.Printf("\ncompare %v [%d,%d) size:%d againSame:%v againCorrect:%v\n", fh.mirrorFile.Name(), offset, offset+totalRead, totalRead, againSame, againCorrect)
			//fmt.Printf("read mirrow data: %v\n", mirrorData)
			//fmt.Printf("read actual data: %v\n", againBuff[:totalRead])
		}
	}

	return fuse.ReadResultData(buff[:totalRead]), fuse.OK
}

func readDataByFileHandle(buff []byte, fhIn *FileHandle, offset int64) (int64, error) {
	// read data from source file
	size := len(buff)
	fhIn.lockForRead(offset, size)
	defer fhIn.unlockForRead(offset, size)

	n, tsNs, err := fhIn.readFromChunks(buff, offset)
	if err == nil || err == io.EOF {
		maxStop := fhIn.readFromDirtyPages(buff, offset, tsNs)
		n = max(maxStop-offset, n)
	}
	if err == io.EOF {
		err = nil
	}
	return n, err
}
