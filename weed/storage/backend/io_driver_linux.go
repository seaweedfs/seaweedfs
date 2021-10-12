// +build linux

package backend

import (
	"os"

	"github.com/iceber/iouring-go"
)

var (
	_ IODriver = &IOUringDriver{}
)

type IOUringDriver struct {
	File      *os.File
	fd        int
	iour      *iouring.IOURing
	rateLimit chan struct{}
}

func init() {
	supportIOUring = true
}

func NewIOUringDriver(file *os.File) (*IOUringDriver, error) {
	iour, err := iouring.New(IOUringEntries)
	if err != nil {
		return nil, err
	}
	rateLimit := make(chan struct{}, IOUringEntries+1)
	for i := uint(0); i < IOUringEntries; i++ {
		rateLimit <- struct{}{}
	}
	return &IOUringDriver{
		File:      file,
		fd:        int(file.Fd()),
		iour:      iour,
		rateLimit: rateLimit,
	}, nil
}

func (d *IOUringDriver) returnToken(token struct{}) {
	d.rateLimit <- token
}

func (d *IOUringDriver) ReadAt(p []byte, off int64) (int, error) {
	token := <-d.rateLimit
	defer d.returnToken(token)
	ch := make(chan iouring.Result, 1)
	preq := iouring.Pread(d.fd, p, uint64(off))
	if _, err := d.iour.SubmitRequests([]iouring.PrepRequest{preq}, ch); err != nil {
		return -1, err
	}
	result := <-ch
	return result.ReturnInt()
}

func (d *IOUringDriver) WriteAt(p []byte, off int64) (int, error) {
	token := <-d.rateLimit
	defer d.returnToken(token)
	ch := make(chan iouring.Result, 1)
	preq := iouring.Pwrite(d.fd, p, uint64(off))
	if _, err := d.iour.SubmitRequest(preq, ch); err != nil {
		return 0, err
	}
	result := <-ch
	return result.ReturnInt()
}

func (d *IOUringDriver) Sync() error {
	token := <-d.rateLimit
	defer d.returnToken(token)
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
