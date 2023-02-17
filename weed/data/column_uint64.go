package data

import (
	"encoding/binary"
	"fmt"
	"io"
)

type ColumnUint64 struct {
}

const SIZE_Uint64 = 8

func (c *ColumnUint64) Read(buf []byte, readerAt io.ReaderAt, offset int64, i int64) uint64 {
	if n, err := readerAt.ReadAt(buf, offset+i*SIZE_Uint64); n == SIZE_Uint64 && err == nil {
		return binary.BigEndian.Uint64(buf)
	}
	return 0
}

func WriteUint64s(buf []byte, data []uint64) (err error) {
	off := 0
	size := len(data)
	if len(buf) < size<<3 {
		return fmt.Errorf("buf too small")
	}
	for _, dat := range data {
		binary.BigEndian.PutUint64(buf[off:], dat)
		off += SIZE_Uint64
	}
	return nil
}
