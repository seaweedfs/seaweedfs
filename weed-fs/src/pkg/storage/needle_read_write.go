package storage

import (
	"errors"
	"io"
	"os"
	"pkg/util"
)

func (n *Needle) Append(w io.Writer, version Version) uint32 {
	if version == Version1 {
		header := make([]byte, NeedleHeaderSize)
		util.Uint32toBytes(header[0:4], n.Cookie)
		util.Uint64toBytes(header[4:12], n.Id)
		n.Size = uint32(len(n.Data))
		util.Uint32toBytes(header[12:16], n.Size)
		w.Write(header)
		w.Write(n.Data)
		rest := NeedlePaddingSize - ((NeedleHeaderSize + n.Size + 4) % NeedlePaddingSize)
		util.Uint32toBytes(header[0:4], n.Checksum.Value())
		w.Write(header[0 : 4+rest])
	} else if version == Version2 {
	}
	return n.Size
}
func (n *Needle) Read(r io.Reader, size uint32, version Version) (int, error) {
	if version == Version1 {
		bytes := make([]byte, NeedleHeaderSize+size+4)
		ret, e := r.Read(bytes)
		n.Cookie = util.BytesToUint32(bytes[0:4])
		n.Id = util.BytesToUint64(bytes[4:12])
		n.Size = util.BytesToUint32(bytes[12:16])
		n.Data = bytes[16 : 16+size]
		checksum := util.BytesToUint32(bytes[16+size : 16+size+4])
		if checksum != NewCRC(n.Data).Value() {
			return 0, errors.New("CRC error! Data On Disk Corrupted!")
		}
		return ret, e
	} else if version == Version2 {
	}
	return 0, errors.New("Unsupported Version!")
}
func ReadNeedleHeader(r *os.File, version Version) (n *Needle, bodyLength uint32) {
	n = new(Needle)
	if version == Version1 {
		bytes := make([]byte, NeedleHeaderSize)
		count, e := r.Read(bytes)
		if count <= 0 || e != nil {
			return nil, 0
		}
		n.Cookie = util.BytesToUint32(bytes[0:4])
		n.Id = util.BytesToUint64(bytes[4:12])
		n.Size = util.BytesToUint32(bytes[12:16])
		rest := NeedlePaddingSize - ((n.Size + 16 + 4) % NeedlePaddingSize)
		bodyLength = n.Size + 4 + rest
	} else if version == Version2 {
	}
	return
}

//n should be a needle already read the header
//the input stream will read until next file entry
func (n *Needle) ReadNeedleBody(r *os.File, version Version, bodyLength uint32) {
	if version == Version1 {
		bytes := make([]byte, bodyLength)
		r.Read(bytes)
		n.Data = bytes[:n.Size]
		n.Checksum = NewCRC(n.Data)
	} else if version == Version2 {
	}
	return
}
