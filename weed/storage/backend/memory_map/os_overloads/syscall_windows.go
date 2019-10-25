// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Windows system calls.

package os_overloads

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// windows api calls

//sys	CreateFile(name *uint16, access uint32, mode uint32, sa *SecurityAttributes, createmode uint32, attrs uint32, templatefile int32) (handle Handle, err error) [failretval==InvalidHandle] = CreateFileW

func makeInheritSa() *syscall.SecurityAttributes {
	var sa syscall.SecurityAttributes
	sa.Length = uint32(unsafe.Sizeof(sa))
	sa.InheritHandle = 1
	return &sa
}

// opens the
func Open(path string, mode int, perm uint32, setToTempAndDelete bool) (fd syscall.Handle, err error) {
	if len(path) == 0 {
		return syscall.InvalidHandle, windows.ERROR_FILE_NOT_FOUND
	}
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return syscall.InvalidHandle, err
	}
	var access uint32
	switch mode & (windows.O_RDONLY | windows.O_WRONLY | windows.O_RDWR) {
	case windows.O_RDONLY:
		access = windows.GENERIC_READ
	case windows.O_WRONLY:
		access = windows.GENERIC_WRITE
	case windows.O_RDWR:
		access = windows.GENERIC_READ | windows.GENERIC_WRITE
	}
	if mode&windows.O_CREAT != 0 {
		access |= windows.GENERIC_WRITE
	}
	if mode&windows.O_APPEND != 0 {
		access &^= windows.GENERIC_WRITE
		access |= windows.FILE_APPEND_DATA
	}
	sharemode := uint32(windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE)
	var sa *syscall.SecurityAttributes
	if mode&windows.O_CLOEXEC == 0 {
		sa = makeInheritSa()
	}
	var createmode uint32
	switch {
	case mode&(windows.O_CREAT|windows.O_EXCL) == (windows.O_CREAT | windows.O_EXCL):
		createmode = windows.CREATE_NEW
	case mode&(windows.O_CREAT|windows.O_TRUNC) == (windows.O_CREAT | windows.O_TRUNC):
		createmode = windows.CREATE_ALWAYS
	case mode&windows.O_CREAT == windows.O_CREAT:
		createmode = windows.OPEN_ALWAYS
	case mode&windows.O_TRUNC == windows.O_TRUNC:
		createmode = windows.TRUNCATE_EXISTING
	default:
		createmode = windows.OPEN_EXISTING
	}

	var h syscall.Handle
	var e error

	if setToTempAndDelete {
		h, e = syscall.CreateFile(pathp, access, sharemode, sa, createmode, (windows.FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_DELETE_ON_CLOSE), 0)
	} else {
		h, e = syscall.CreateFile(pathp, access, sharemode, sa, createmode, windows.FILE_ATTRIBUTE_NORMAL, 0)
	}
	return h, e
}
