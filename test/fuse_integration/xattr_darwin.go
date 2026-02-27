//go:build darwin

package fuse

import (
	"syscall"
	"unsafe"
)

// Extended attributes support for macOS

const (
	// macOS-specific flags
	XATTR_NOFOLLOW = 0x0001
	XATTR_CREATE   = 0x0002
	XATTR_REPLACE  = 0x0004
)

func setXattr(path, name string, value []byte, flags int) error {
	pathBytes, err := syscall.BytePtrFromString(path)
	if err != nil {
		return err
	}
	nameBytes, err := syscall.BytePtrFromString(name)
	if err != nil {
		return err
	}

	var valuePtr unsafe.Pointer
	if len(value) > 0 {
		valuePtr = unsafe.Pointer(&value[0])
	}

	_, _, errno := syscall.Syscall6(syscall.SYS_SETXATTR,
		uintptr(unsafe.Pointer(pathBytes)),
		uintptr(unsafe.Pointer(nameBytes)),
		uintptr(valuePtr),
		uintptr(len(value)),
		uintptr(0), // position (not used for regular files)
		uintptr(flags))

	if errno != 0 {
		return errno
	}
	return nil
}

func getXattr(path, name string, value []byte) (int, error) {
	pathBytes, err := syscall.BytePtrFromString(path)
	if err != nil {
		return 0, err
	}
	nameBytes, err := syscall.BytePtrFromString(name)
	if err != nil {
		return 0, err
	}

	var valuePtr unsafe.Pointer
	if len(value) > 0 {
		valuePtr = unsafe.Pointer(&value[0])
	}

	size, _, errno := syscall.Syscall6(syscall.SYS_GETXATTR,
		uintptr(unsafe.Pointer(pathBytes)),
		uintptr(unsafe.Pointer(nameBytes)),
		uintptr(valuePtr),
		uintptr(len(value)),
		uintptr(0), // position
		uintptr(0)) // options

	if errno != 0 {
		return 0, errno
	}
	return int(size), nil
}

func listXattr(path string, list []byte) (int, error) {
	pathBytes, err := syscall.BytePtrFromString(path)
	if err != nil {
		return 0, err
	}

	var listPtr unsafe.Pointer
	if len(list) > 0 {
		listPtr = unsafe.Pointer(&list[0])
	}

	size, _, errno := syscall.Syscall6(syscall.SYS_LISTXATTR,
		uintptr(unsafe.Pointer(pathBytes)),
		uintptr(listPtr),
		uintptr(len(list)),
		uintptr(0), // options
		0, 0)

	if errno != 0 {
		return 0, errno
	}
	return int(size), nil
}

func removeXattr(path, name string) error {
	pathBytes, err := syscall.BytePtrFromString(path)
	if err != nil {
		return err
	}
	nameBytes, err := syscall.BytePtrFromString(name)
	if err != nil {
		return err
	}

	_, _, errno := syscall.Syscall6(syscall.SYS_REMOVEXATTR,
		uintptr(unsafe.Pointer(pathBytes)),
		uintptr(unsafe.Pointer(nameBytes)),
		uintptr(0), // options
		0, 0, 0)

	if errno != 0 {
		return errno
	}
	return nil
}

func isXattrSupported() bool {
	return true
}
