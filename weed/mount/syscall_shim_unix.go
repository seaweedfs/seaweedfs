//go:build !windows

package mount

import (
	"syscall"

	sys "golang.org/x/sys/unix"
)

const (
	f_RDLCK   = syscall.F_RDLCK
	f_WRLCK   = syscall.F_WRLCK
	f_UNLCK   = syscall.F_UNLCK
	o_ACCMODE = syscall.O_ACCMODE

	xattr_CREATE  = sys.XATTR_CREATE
	xattr_REPLACE = sys.XATTR_REPLACE
)
