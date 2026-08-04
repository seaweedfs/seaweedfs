//go:build !windows

package winfsp

type handle uintptr

func createDeleteOnClose(path string) (handle, error) { return 0, errWindowsOnly }

func closeHandle(h handle) error { return errWindowsOnly }
