//go:build !windows

package winfsp

import "errors"

func getDiskFreeSpace(path string, free, total, totalFree *uint64) error {
	return errors.New("windows only")
}
