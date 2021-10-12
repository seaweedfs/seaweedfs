// +build !linux

package backend

import (
    "fmt"
    "os"
)

var (
    errUnsupportIODriver = fmt.Errorf("Unsupport IO driver")
)

func NewIOUringDriver(file *os.File) (IODriver, error) {
    return nil, errUnsupportIODriver
}
