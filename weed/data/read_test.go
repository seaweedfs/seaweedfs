package data

import (
	"encoding/binary"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"testing"
)

func TestRead(t *testing.T) {
	x := make([]uint16, 128)
	y := make([]uint32, 128)
	z := make([]uint64, 128)

	for i := range x {
		x[i] = uint16(i)
	}
	for i := range y {
		y[i] = uint32(i * 2)
	}
	for i := range z {
		z[i] = uint64(i * 3)
	}

	xbuf := make([]byte, len(x)*SIZE_Uint16)
	ybuf := make([]byte, len(x)*SIZE_Uint32)
	zbuf := make([]byte, len(x)*SIZE_Uint64)

	WriteUint16s(xbuf, x)
	WriteUint32s(ybuf, y)
	WriteUint64s(zbuf, z)

	df := &DataFile{
		xbuf:      xbuf,
		ybuf:      ybuf,
		zbuf:      zbuf,
		xLen:      len(xbuf),
		yLen:      len(ybuf),
		zLen:      len(zbuf),
		xReaderAt: util.NewBytesReader(xbuf),
		yReaderAt: util.NewBytesReader(ybuf),
		zReaderAt: util.NewBytesReader(zbuf),
	}

	dataLayout := make(map[FieldName]DataLayout)
	dataLayout["x"] = DataLayout{
		LayoutType: Uint16,
		SortType:   Unsorted,
	}
	dataLayout["y"] = DataLayout{
		LayoutType: Uint32,
		SortType:   Unsorted,
	}
	dataLayout["z"] = DataLayout{
		LayoutType: Uint64,
		SortType:   Unsorted,
	}

	rows, err := df.ReadRows("x", dataLayout, Equal, NewDUint16(65))
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}
	for _, row := range rows {
		fmt.Printf("row %d width %d ", row.index, len(row.Datums))
		for i, d := range row.Datums {
			fmt.Printf("%d: %v ", i, d)
		}
		fmt.Println()
	}

}

type Operator int32
type LayoutType int32
type SortType int32

const (
	Equal Operator = 0
	GreaterThan
	GreaterOrEqual
	LessThan
	LessOrEqual

	Uint16 LayoutType = 0
	Uint32            = 1
	Uint64            = 2

	Unsorted SortType = 0
	Ascending
	Descending
)

type DataFile struct {
	xbuf      []byte
	ybuf      []byte
	zbuf      []byte
	xReaderAt io.ReaderAt
	yReaderAt io.ReaderAt
	zReaderAt io.ReaderAt
	xLen      int
	yLen      int
	zLen      int
}

type DataLayout struct {
	LayoutType
	SortType
}

type FieldName string

func (d *DataFile) ReadRows(field FieldName, layout map[FieldName]DataLayout, op Operator, operand Datum) (rows []*Row, err error) {
	if field == "x" {
		rows, err = pushDownReadRows(d.xReaderAt, d.xLen, layout[field], op, operand)
		if err != nil {
			return
		}
		err = hydrateRows(d.yReaderAt, d.yLen, layout["y"], rows)
		if err != nil {
			return
		}
		err = hydrateRows(d.zReaderAt, d.zLen, layout["z"], rows)
		if err != nil {
			return
		}
	}
	if field == "y" {
		rows, err = pushDownReadRows(d.yReaderAt, d.yLen, layout[field], op, operand)
		if err != nil {
			return
		}
		err = hydrateRows(d.xReaderAt, d.xLen, layout["x"], rows)
		if err != nil {
			return
		}
		err = hydrateRows(d.zReaderAt, d.zLen, layout["z"], rows)
		if err != nil {
			return
		}
	}
	return
}

type Row struct {
	index int
	Datums
}

func pushDownReadRows(readerAt io.ReaderAt, dataLen int, layout DataLayout, op Operator, operand Datum) (rows []*Row, err error) {
	if layout.LayoutType == Uint16 {
		if layout.SortType == Unsorted {
			buf := make([]byte, SIZE_Uint16)
			for i := 0; i < dataLen; i += SIZE_Uint16 {
				if n, err := readerAt.ReadAt(buf, int64(i)); n == SIZE_Uint16 && err == nil {
					d := NewDUint16(DUint16(binary.BigEndian.Uint16(buf)))
					cmp, err := d.Compare(operand)
					if err != nil {
						return rows, err
					}
					if cmp == 0 && op == Equal {
						println(1)
						rows = append(rows, &Row{
							index:  i / SIZE_Uint16,
							Datums: []Datum{d},
						})
					}
				}
			}
		}
	}
	if layout.LayoutType == Uint32 {
		if layout.SortType == Unsorted {
			buf := make([]byte, SIZE_Uint32)
			for i := 0; i < dataLen; i += SIZE_Uint32 {
				if n, err := readerAt.ReadAt(buf, int64(i)); n == SIZE_Uint32 && err == nil {
					d := NewDUint32(DUint32(binary.BigEndian.Uint32(buf)))
					cmp, err := d.Compare(operand)
					if err != nil {
						return rows, err
					}
					if cmp == 0 && op == Equal {
						println(2)
						rows = append(rows, &Row{
							index:  i / SIZE_Uint32,
							Datums: []Datum{d},
						})
					}
				}
			}
		}
	}
	if layout.LayoutType == Uint64 {
		if layout.SortType == Unsorted {
			buf := make([]byte, SIZE_Uint64)
			for i := 0; i < dataLen; i += SIZE_Uint64 {
				if n, err := readerAt.ReadAt(buf, int64(i)); n == SIZE_Uint64 && err == nil {
					d := NewDUint64(DUint64(binary.BigEndian.Uint64(buf)))
					cmp, err := d.Compare(operand)
					if err != nil {
						return rows, err
					}
					if cmp == 0 && op == Equal {
						println(2)
						rows = append(rows, &Row{
							index:  i / SIZE_Uint64,
							Datums: []Datum{d},
						})
					}
				}
			}
		}
	}
	return
}

func hydrateRows(readerAt io.ReaderAt, dataLen int, layout DataLayout, rows []*Row) (err error) {
	if layout.LayoutType == Uint16 {
		if layout.SortType == Unsorted {
			buf := make([]byte, SIZE_Uint16)
			for _, row := range rows {
				if n, err := readerAt.ReadAt(buf, int64(row.index)*SIZE_Uint16); n == SIZE_Uint16 && err == nil {
					t := binary.BigEndian.Uint16(buf)
					d := NewDUint16(DUint16(t))
					println(3, "add", t)
					row.Datums = append(row.Datums, d)
				}
			}
		}
	}
	if layout.LayoutType == Uint32 {
		if layout.SortType == Unsorted {
			buf := make([]byte, SIZE_Uint32)
			for _, row := range rows {
				if n, err := readerAt.ReadAt(buf, int64(row.index)*SIZE_Uint32); n == SIZE_Uint32 && err == nil {
					t := binary.BigEndian.Uint32(buf)
					d := NewDUint32(DUint32(t))
					println(4, "add", t)
					row.Datums = append(row.Datums, d)
				}
			}
		}
	}
	if layout.LayoutType == Uint64 {
		if layout.SortType == Unsorted {
			buf := make([]byte, SIZE_Uint64)
			for _, row := range rows {
				if n, err := readerAt.ReadAt(buf, int64(row.index)*SIZE_Uint64); n == SIZE_Uint64 && err == nil {
					t := binary.BigEndian.Uint64(buf)
					d := NewDUint64(DUint64(t))
					println(5, "add", t)
					row.Datums = append(row.Datums, d)
				}
			}
		}
	}
	return
}
