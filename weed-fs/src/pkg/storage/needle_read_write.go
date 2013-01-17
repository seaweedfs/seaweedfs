package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"pkg/util"
)

func (n *Needle) Append(w io.Writer, version Version) uint32 {
	switch version {
	case Version1:
		header := make([]byte, NeedleHeaderSize)
		util.Uint32toBytes(header[0:4], n.Cookie)
		util.Uint64toBytes(header[4:12], n.Id)
		n.Size = uint32(len(n.Data))
		util.Uint32toBytes(header[12:16], n.Size)
		w.Write(header)
		w.Write(n.Data)
		padding := NeedlePaddingSize - ((NeedleHeaderSize + n.Size + NeedleChecksumSize) % NeedlePaddingSize)
		util.Uint32toBytes(header[0:NeedleChecksumSize], n.Checksum.Value())
		w.Write(header[0 : NeedleChecksumSize+padding])
		return n.Size
	case Version2:
		header := make([]byte, NeedleHeaderSize)
		util.Uint32toBytes(header[0:4], n.Cookie)
		util.Uint64toBytes(header[4:12], n.Id)
		n.DataSize, n.NameSize, n.MimeSize = uint32(len(n.Data)), uint8(len(n.Name)), uint8(len(n.Mime))
		if n.DataSize > 0 {
			n.Size = 4 + n.DataSize + 1
			if n.HasName() {
				n.Size = n.Size + 1 + uint32(n.NameSize)
			}
			if n.HasMime() {
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
		if n.HasName() {
			util.Uint8toBytes(header[0:1], n.NameSize)
			w.Write(header[0:1])
			w.Write(n.Name)
		}
		if n.HasMime() {
			util.Uint8toBytes(header[0:1], n.MimeSize)
			w.Write(header[0:1])
			w.Write(n.Mime)
		}
		padding := NeedlePaddingSize - ((NeedleHeaderSize + n.Size + NeedleChecksumSize) % NeedlePaddingSize)
		util.Uint32toBytes(header[0:NeedleChecksumSize], n.Checksum.Value())
		w.Write(header[0 : NeedleChecksumSize+padding])
		return n.DataSize
	}
	return n.Size
}
func (n *Needle) Read(r io.Reader, size uint32, version Version) (int, error) {
	switch version {
	case Version1:
		bytes := make([]byte, NeedleHeaderSize+size+NeedleChecksumSize)
		ret, e := r.Read(bytes)
		n.readNeedleHeader(bytes)
		n.Data = bytes[NeedleHeaderSize : NeedleHeaderSize+size]
		checksum := util.BytesToUint32(bytes[NeedleHeaderSize+size : NeedleHeaderSize+size+NeedleChecksumSize])
		if checksum != NewCRC(n.Data).Value() {
			return 0, errors.New("CRC error! Data On Disk Corrupted!")
		}
		return ret, e
	case Version2:
		if size == 0 {
			return 0, nil
		}
		bytes := make([]byte, NeedleHeaderSize+size+NeedleChecksumSize)
		ret, e := r.Read(bytes)
		if e != nil {
			return 0, e
		}
		if ret != int(NeedleHeaderSize+size+NeedleChecksumSize) {
			return 0, errors.New("File Entry Not Found!")
		}
		n.readNeedleHeader(bytes)
		if n.Size != size {
			return 0, fmt.Errorf("File Entry Not Found! Needle %d Memory %d", n.Size, size)
		}
		n.readNeedleDataVersion2(bytes[NeedleHeaderSize : NeedleHeaderSize+int(n.Size)])
		checksum := util.BytesToUint32(bytes[NeedleHeaderSize+n.Size : NeedleHeaderSize+n.Size+NeedleChecksumSize])
		if checksum != NewCRC(n.Data).Value() {
			return 0, errors.New("CRC error! Data On Disk Corrupted!")
		}
		return ret, e
	}
	return 0, fmt.Errorf("Unsupported Version! (%d)", version)
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
	if index < lenBytes && n.HasName() {
		n.NameSize = uint8(bytes[index])
		index = index + 1
		n.Name = bytes[index : index+int(n.NameSize)]
		index = index + int(n.NameSize)
	}
	if index < lenBytes && n.HasMime() {
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
		padding := NeedlePaddingSize - ((n.Size + NeedleHeaderSize + NeedleChecksumSize) % NeedlePaddingSize)
		bodyLength = n.Size + NeedleChecksumSize + padding
	}
	return
}

//n should be a needle already read the header
//the input stream will read until next file entry
func (n *Needle) ReadNeedleBody(r *os.File, version Version, bodyLength uint32) {
	switch version {
	case Version1:
		bytes := make([]byte, bodyLength)
		r.Read(bytes)
		n.Data = bytes[:n.Size]
		n.Checksum = NewCRC(n.Data)
	case Version2:
		bytes := make([]byte, bodyLength)
		r.Read(bytes)
		n.readNeedleDataVersion2(bytes[0:n.Size])
		n.Checksum = NewCRC(n.Data)
	}
	return
}

func (n *Needle) IsGzipped() bool {
	return n.Flags&0x01 == 0x01
}
func (n *Needle) SetGzipped() {
	n.Flags = n.Flags | 0x01
}
func (n *Needle) HasName() bool {
	return n.Flags&0x02 == 0x02
}
func (n *Needle) SetHasName() {
	n.Flags = n.Flags | 0x02
}
func (n *Needle) HasMime() bool {
	return n.Flags&0x04 == 0x04
}
func (n *Needle) SetHasMime() {
	n.Flags = n.Flags | 0x04
}
