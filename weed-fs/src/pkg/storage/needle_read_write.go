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
		return n.Size
	} else if version == Version2 {
		header := make([]byte, NeedleHeaderSize)
		util.Uint32toBytes(header[0:4], n.Cookie)
		util.Uint64toBytes(header[4:12], n.Id)
		n.DataSize, n.NameSize, n.MimeSize = uint32(len(n.Data)), uint8(len(n.Name)), uint8(len(n.Mime))
		if n.DataSize > 0 {
			n.Size = 4 + n.DataSize + 1
			if n.NameSize > 0 {
				n.Size = n.Size + 1 + uint32(n.NameSize)
			}
			if n.MimeSize > 0 {
				n.Size = n.Size + 1 + uint32(n.MimeSize)
			}
		}
		util.Uint32toBytes(header[12:16], n.Size)
		w.Write(header)
		if n.DataSize > 0 {
			util.Uint32toBytes(header[0:4], n.DataSize)
			w.Write(header[0:4])
			w.Write(n.Data)
			util.Uint8toBytes(header[0:1], n.Flags)
			w.Write(header[0:1])
		}
		if n.NameSize > 0 {
			util.Uint8toBytes(header[0:1], n.NameSize)
			w.Write(header[0:1])
			w.Write(n.Name)
		}
		if n.MimeSize > 0 {
			util.Uint8toBytes(header[0:1], n.MimeSize)
			w.Write(header[0:1])
			w.Write(n.Mime)
		}
		rest := NeedlePaddingSize - ((NeedleHeaderSize + n.Size + 4) % NeedlePaddingSize)
		util.Uint32toBytes(header[0:4], n.Checksum.Value())
		w.Write(header[0 : 4+rest])
		return n.DataSize
	}
	return n.Size
}
func (n *Needle) Read(r io.Reader, size uint32, version Version) (int, error) {
	if version == Version1 {
		bytes := make([]byte, NeedleHeaderSize+size+4)
		ret, e := r.Read(bytes)
		n.readNeedleHeader(bytes)
		n.Data = bytes[NeedleHeaderSize : NeedleHeaderSize+size]
		checksum := util.BytesToUint32(bytes[NeedleHeaderSize+size : NeedleHeaderSize+size+4])
		if checksum != NewCRC(n.Data).Value() {
			return 0, errors.New("CRC error! Data On Disk Corrupted!")
		}
		return ret, e
	} else if version == Version2 {
		bytes := make([]byte, NeedleHeaderSize+size+4)
		ret, e := r.Read(bytes)
		n.readNeedleHeader(bytes)
		n.readNeedleDataVersion2(bytes[NeedleHeaderSize : NeedleHeaderSize+int(n.Size)])
		checksum := util.BytesToUint32(bytes[NeedleHeaderSize+n.Size : NeedleHeaderSize+n.Size+4])
		if checksum != NewCRC(n.Data).Value() {
			return 0, errors.New("CRC error! Data On Disk Corrupted!")
		}
		return ret, e
	}
	return 0, errors.New("Unsupported Version!")
}
func (n *Needle) readNeedleHeader(bytes []byte) {
	n.Cookie = util.BytesToUint32(bytes[0:4])
	n.Id = util.BytesToUint64(bytes[4:12])
	n.Size = util.BytesToUint32(bytes[12:NeedleHeaderSize])
}
func (n *Needle) readNeedleDataVersion2(bytes []byte) {
	index, lenBytes := 0, len(bytes)
	if index < lenBytes {
		n.DataSize = util.BytesToUint32(bytes[index : index+4])
		index = index + 4
		n.Data = bytes[index : index+int(n.DataSize)]
		index = index + int(n.DataSize)
		n.Flags = bytes[index]
		index = index + 1
	}
	if index < lenBytes {
		n.NameSize = uint8(bytes[index])
		index = index + 1
		n.Name = bytes[index : index+int(n.NameSize)]
		index = index + int(n.NameSize)
	}
	if index < lenBytes {
		n.MimeSize = uint8(bytes[index])
		index = index + 1
		n.Mime = bytes[index : index+int(n.MimeSize)]
	}
}
func ReadNeedleHeader(r *os.File, version Version) (n *Needle, bodyLength uint32) {
	n = new(Needle)
	if version == Version1 || version == Version2 {
		bytes := make([]byte, NeedleHeaderSize)
		count, e := r.Read(bytes)
		if count <= 0 || e != nil {
			return nil, 0
		}
		n.readNeedleHeader(bytes)
		rest := NeedlePaddingSize - ((n.Size + NeedleHeaderSize + 4) % NeedlePaddingSize)
		bodyLength = n.Size + 4 + rest
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
		bytes := make([]byte, bodyLength)
		r.Read(bytes)
		n.readNeedleDataVersion2(bytes[0:n.Size])
		n.Checksum = NewCRC(n.Data)
	}
	return
}
