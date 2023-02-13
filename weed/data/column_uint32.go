package data

import (
	"encoding/binary"
	"fmt"
	"io"
)

type ColumnUint32 struct {
}

const SIZE_Uint32 = 4

func (c *ColumnUint32) Read(buf []byte, readerAt io.ReaderAt, offset int64, i int64) uint32 {
	if n, err := readerAt.ReadAt(buf, offset+i*SIZE_Uint32); n == SIZE_Uint32 && err == nil {
		return binary.BigEndian.Uint32(buf)
	}
	return 0
}

func WriteUint32s(buf []byte, data []uint32) (err error) {
	off := 0
	size := len(data)
	if len(buf) < size<<2 {
		return fmt.Errorf("buf too small")
	}
	for _, dat := range data {
		binary.BigEndian.PutUint32(buf[off:], dat)
		off += SIZE_Uint32
	}
	return nil
}
