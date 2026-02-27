//go:build linux

package fuse

import (
	"syscall"
	"unsafe"
)

// Extended attributes support for Linux

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
		uintptr(flags),
		0)

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
		0,
		0)

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

	size, _, errno := syscall.Syscall(syscall.SYS_LISTXATTR,
		uintptr(unsafe.Pointer(pathBytes)),
		uintptr(listPtr),
		uintptr(len(list)))

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

	_, _, errno := syscall.Syscall(syscall.SYS_REMOVEXATTR,
		uintptr(unsafe.Pointer(pathBytes)),
		uintptr(unsafe.Pointer(nameBytes)),
		0)

	if errno != 0 {
		return errno
	}
	return nil
}

func isXattrSupported() bool {
	return true
}
