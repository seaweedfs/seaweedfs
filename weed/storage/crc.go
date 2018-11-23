package storage

import (
	"crypto/md5"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/klauspost/crc32"
)

var table = crc32.MakeTable(crc32.Castagnoli)

type CRC uint32

func NewCRC(b []byte) CRC {
	return CRC(0).Update(b)
}

func (c CRC) Update(b []byte) CRC {
	return CRC(crc32.Update(uint32(c), table, b))
}

func (c CRC) Value() uint32 {
	return uint32(c>>15|c<<17) + 0xa282ead8
}

func (n *Needle) Etag() string {
	bits := make([]byte, 4)
	util.Uint32toBytes(bits, uint32(n.Checksum))
	return fmt.Sprintf("%x", bits)
}

func (n *Needle) MD5() string {

	hash := md5.New()

	hash.Write(n.Data)

	return fmt.Sprintf("%x", hash.Sum(nil))

}
