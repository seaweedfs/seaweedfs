package data

import (
	"encoding/binary"
	"fmt"
	"io"
)

type ColumnUint16 struct {
}

const SIZE_Uint16 = 2

func (c *ColumnUint16) Read(buf []byte, readerAt io.ReaderAt, offset int64, i int64) uint16 {
	if n, err := readerAt.ReadAt(buf, offset+i*SIZE_Uint16); n == SIZE_Uint16 && err == nil {
		return binary.BigEndian.Uint16(buf)
	}
	return 0
}

func WriteUint16s(buf []byte, data []uint16) (err error) {
	off := 0
	size := len(data)
	if len(buf) < size<<1 {
		return fmt.Errorf("buf too small")
	}
	for _, dat := range data {
		binary.BigEndian.PutUint16(buf[off:], dat)
		off += SIZE_Uint16
	}
	return nil
}
