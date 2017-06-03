package storage

import (
	"os"
)

func getBytesForFileBlock(r *os.File, offset int64, readSize int) (dataSlice []byte, err error) {
	dataSlice = make([]byte, readSize)
	_, err = r.ReadAt(dataSlice, offset)
	return dataSlice, err
}
