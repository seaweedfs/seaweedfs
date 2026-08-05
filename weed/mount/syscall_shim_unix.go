//go:build !windows

package mount

import "syscall"

const (
	f_RDLCK   = syscall.F_RDLCK
	f_WRLCK   = syscall.F_WRLCK
	f_UNLCK   = syscall.F_UNLCK
	o_ACCMODE = syscall.O_ACCMODE
)
