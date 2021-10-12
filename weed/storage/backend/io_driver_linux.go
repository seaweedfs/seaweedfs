// +build linux

package backend

import (
    "os"

    "github.com/iceber/iouring-go"
)

const (
    IOURING_SIZE = 256
)

var (
    _ IODriver = &IOUringDriver{}
)

type IOUringDriver struct {
    File *os.File
    fd int
    iour *iouring.IOURing
}

func init() {
    supportIOUring = true
}

func NewIOUringDriver(file *os.File) (*IOUringDriver, error) {
    iour, err := iouring.New(IOURING_SIZE)
    if err != nil {
        return nil, err
    }
    return &IOUringDriver{
        File: file,
        fd: int(file.Fd()),
        iour: iour,
    }, nil
}

func (d *IOUringDriver) ReadAt(p []byte, off int64) (int, error) {
    ch := make(chan iouring.Result, 1)
    preq := iouring.Pread(d.fd, p, uint64(off))
    if _, err := d.iour.SubmitRequest(preq, ch); err != nil {
        return 0, err
    }
    result := <-ch
    return result.ReturnInt()
}

func (d *IOUringDriver) WriteAt(p []byte, off int64) (int, error) {
    ch := make(chan iouring.Result, 1)
    preq := iouring.Pwrite(d.fd, p, uint64(off))
    if _, err := d.iour.SubmitRequest(preq, ch); err != nil {
        return 0, err
    }
    result := <-ch
    return result.ReturnInt()
}

func (d *IOUringDriver) Sync() error {
    ch := make(chan iouring.Result, 1)
    preq := iouring.Fsync(d.fd)
    if _, err := d.iour.SubmitRequest(preq, ch); err != nil {
        return err
    }
    result := <-ch
    return result.Err()
}

func (d *IOUringDriver) Truncate(off int64) error {
    return d.File.Truncate(off)
}

func (d *IOUringDriver) Close() error {
    err1 := d.iour.Close()
    err2 := d.File.Close()
    if err1 != nil {
        if err2 == nil {
            return err1
        }
        return err2
    }
    return err2
}

