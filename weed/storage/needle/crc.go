package needle

import (
	"fmt"
	"io"

	"hash/crc32"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

var table = crc32.MakeTable(crc32.Castagnoli)

type CRC uint32

func NewCRC(b []byte) CRC {
	return CRC(0).Update(b)
}

func (c CRC) Update(b []byte) CRC {
	return CRC(crc32.Update(uint32(c), table, b))
}

// Value Deprecated. Just use the raw uint32 value to compare.
func (c CRC) Value() uint32 {
	return uint32(c>>15|c<<17) + 0xa282ead8
}

func (n *Needle) Etag() string {
	bits := make([]byte, 4)
	util.Uint32toBytes(bits, uint32(n.Checksum))
	return fmt.Sprintf("%x", bits)
}

func NewCRCwriter(w io.Writer) *CRCwriter {

	return &CRCwriter{
		crc: CRC(0),
		w:   w,
	}

}

type CRCwriter struct {
	crc CRC
	w   io.Writer
}

func (c *CRCwriter) Write(p []byte) (n int, err error) {
	n, err = c.w.Write(p) // with each write ...
	c.crc = c.crc.Update(p)
	return
}

func (c *CRCwriter) Sum() uint32 { return uint32(c.crc) } // final hash
