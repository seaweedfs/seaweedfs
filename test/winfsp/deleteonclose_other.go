//go:build !windows

package winfsp

import "errors"

type handle uintptr

var errWindowsOnly = errors.New("windows only")

func createDeleteOnClose(path string) (handle, error) { return 0, errWindowsOnly }
func closeHandle(h handle) error                      { return errWindowsOnly }
