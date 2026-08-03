// Package winfsp mounts a SeaweedFS filesystem on Windows through WinFsp.
//
// WinFsp speaks a path-based FUSE dialect, while weed/mount implements the
// inode-based raw protocol the Linux kernel uses. This package translates
// between the two so both platforms run the same filesystem code.
package winfsp

import (
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

// WinFsp's FUSE layer expects POSIX errno numbers. They are spelled out rather
// than taken from syscall because Go numbers Windows errnos as offsets from
// APPLICATION_ERROR, nothing like the POSIX values the other side reads.
const (
	ePERM   = 1
	eNOENT  = 2
	eINTR   = 4
	eIO     = 5
	eNXIO   = 6
	eBADF   = 9
	eAGAIN  = 11
	eNOMEM  = 12
	eACCES  = 13
	eBUSY   = 16
	eEXIST  = 17
	eXDEV   = 18
	eNODEV  = 19
	eNOTDIR = 20
	eISDIR  = 21
	eINVAL  = 22
	eNFILE  = 23
	eMFILE  = 24
	eFBIG   = 27
	eNOSPC  = 28
	eSPIPE  = 29
	eROFS   = 30
	eMLINK  = 31
	ePIPE   = 32
	eRANGE  = 34

	eNAMETOOLONG = 36
	eNOSYS       = 38
	eNOTEMPTY    = 39
	eLOOP        = 40
	eNODATA      = 61
	eNOTSUP      = 95
)

// statusErrno is keyed by the running platform's errno values, since that is
// what the raw filesystem returns, and yields the POSIX number for the wire.
// Built rather than declared: platforms alias errnos differently (freebsd has
// no ENODATA, linux makes ENOATTR the same value), so entries can collide.
// First one wins, so the general codes keep their meaning.
var statusErrno = buildStatusErrno()

func buildStatusErrno() map[fuse.Status]int {
	table := []struct {
		status fuse.Status
		errno  int
	}{
		{fuse.Status(syscall.EPERM), ePERM},
		{fuse.Status(syscall.ENOENT), eNOENT},
		{fuse.Status(syscall.EINTR), eINTR},
		{fuse.Status(syscall.EIO), eIO},
		{fuse.Status(syscall.ENXIO), eNXIO},
		{fuse.Status(syscall.EBADF), eBADF},
		{fuse.Status(syscall.EAGAIN), eAGAIN},
		{fuse.Status(syscall.ENOMEM), eNOMEM},
		{fuse.Status(syscall.EACCES), eACCES},
		{fuse.Status(syscall.EBUSY), eBUSY},
		{fuse.Status(syscall.EEXIST), eEXIST},
		{fuse.Status(syscall.EXDEV), eXDEV},
		{fuse.Status(syscall.ENODEV), eNODEV},
		{fuse.Status(syscall.ENOTDIR), eNOTDIR},
		{fuse.Status(syscall.EISDIR), eISDIR},
		{fuse.Status(syscall.EINVAL), eINVAL},
		{fuse.Status(syscall.ENFILE), eNFILE},
		{fuse.Status(syscall.EMFILE), eMFILE},
		{fuse.Status(syscall.EFBIG), eFBIG},
		{fuse.Status(syscall.ENOSPC), eNOSPC},
		{fuse.Status(syscall.ESPIPE), eSPIPE},
		{fuse.Status(syscall.EROFS), eROFS},
		{fuse.Status(syscall.EMLINK), eMLINK},
		{fuse.Status(syscall.EPIPE), ePIPE},
		{fuse.Status(syscall.ERANGE), eRANGE},
		{fuse.Status(syscall.ENAMETOOLONG), eNAMETOOLONG},
		{fuse.Status(syscall.ENOSYS), eNOSYS},
		{fuse.Status(syscall.ENOTEMPTY), eNOTEMPTY},
		{fuse.Status(syscall.ELOOP), eLOOP},
		{fuse.ENOTSUP, eNOTSUP},
		{fuse.ENODATA, eNODATA},
		{fuse.ENOATTR, eNODATA},
	}
	m := make(map[fuse.Status]int, len(table))
	for _, entry := range table {
		if _, exists := m[entry.status]; !exists {
			m[entry.status] = entry.errno
		}
	}
	return m
}

// toErrno converts a raw filesystem status into the negative errno WinFsp
// wants. Unrecognised failures become -EIO rather than passing through a
// number that means something else on the other side.
func toErrno(status fuse.Status) int {
	if status == fuse.OK {
		return 0
	}
	if n, ok := statusErrno[status]; ok {
		return -n
	}
	return -eIO
}
