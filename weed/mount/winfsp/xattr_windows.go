package winfsp

import (
	"bytes"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// xattrBufferSize bounds one attribute value and one name listing. The mount
// stores extended attributes in the entry itself, so they are small by
// construction; this is the ceiling WinFsp will accept in a single reply.
const xattrBufferSize = 64 * 1024

func (w *WinFS) Getxattr(path string, name string) (int, []byte) {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status), nil
	}

	dest := make([]byte, xattrBufferSize)
	size, status := w.wfs.GetXAttr(never, ptr(w.caller(inode)), name, dest)
	if status != fuse.OK {
		return toErrno(status), nil
	}
	return 0, dest[:size]
}

func (w *WinFS) Setxattr(path string, name string, value []byte, flags int) int {
	if w.denied() {
		return -eROFS
	}
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}

	// cgofuse and the raw filesystem number XATTR_CREATE and XATTR_REPLACE the
	// same, so the flags pass straight through; TestXattrFlagValues pins that.
	in := &fuse.SetXAttrIn{InHeader: w.caller(inode)}
	in.Flags = uint32(flags)
	in.Size = uint32(len(value))
	return toErrno(w.wfs.SetXAttr(never, in, name, value))
}

func (w *WinFS) Removexattr(path string, name string) int {
	if w.denied() {
		return -eROFS
	}
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}
	return toErrno(w.wfs.RemoveXAttr(never, ptr(w.caller(inode)), name))
}

func (w *WinFS) Listxattr(path string, fill func(name string) bool) int {
	inode, status := w.resolve(path)
	if status != fuse.OK {
		return toErrno(status)
	}

	dest := make([]byte, xattrBufferSize)
	size, status := w.wfs.ListXAttr(never, ptr(w.caller(inode)), dest)
	if status != fuse.OK {
		return toErrno(status)
	}
	for _, name := range splitXattrNames(dest[:size]) {
		if !fill(name) {
			break
		}
	}
	return 0
}

// splitXattrNames unpacks the NUL-separated listing the raw filesystem
// produces. A trailing NUL terminates the last name rather than starting an
// empty one.
func splitXattrNames(packed []byte) []string {
	var names []string
	for _, name := range bytes.Split(packed, []byte{0}) {
		if len(name) > 0 {
			names = append(names, string(name))
		}
	}
	return names
}
