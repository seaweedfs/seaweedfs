package needle

import (
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// NeedleReadOptions specifies which parts of the Needle to read.
type NeedleReadOptions struct {
	ReadHeader bool // always true for any read
	ReadData   bool // read the Data field
	ReadMeta   bool // read metadata fields (Name, Mime, LastModified, Ttl, Pairs, etc.)
}

// ReadFromFile reads the Needle from the backend file according to the specified options.
// - If only ReadHeader is true, only the header is read and parsed.
// - If ReadData or ReadMeta is true, reads GetActualSize(size, version) bytes from disk (size is the logical body size).
func (n *Needle) ReadFromFile(r backend.BackendStorageFile, offset int64, size Size, version Version, opts NeedleReadOptions) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic occurred: %+v", r)
		}
	}()

	if opts.ReadHeader && !opts.ReadData && !opts.ReadMeta {
		// Only read the header
		header := make([]byte, NeedleHeaderSize)
		count, err := r.ReadAt(header, offset)
		if err == io.EOF && count == NeedleHeaderSize {
			err = nil
		}
		if count != NeedleHeaderSize || err != nil {
			return err
		}
		n.ParseNeedleHeader(header)
		return nil
	}
	if opts.ReadHeader && opts.ReadMeta && !opts.ReadData {
		// Optimized: Read header and DataSize in one call
		buf := make([]byte, NeedleHeaderSize+DataSizeSize)
		count, err := r.ReadAt(buf, offset)
		if err == io.EOF && count == NeedleHeaderSize+DataSizeSize {
			err = nil
		}
		if count != NeedleHeaderSize+DataSizeSize || err != nil {
			return err
		}
		n.ParseNeedleHeader(buf[:NeedleHeaderSize])
		if n.Size != size {
			if OffsetSize == 4 && offset < int64(MaxPossibleVolumeSize) {
				return ErrorSizeMismatch
			}
		}

		// Now read meta fields after DataSize+Data
		if version == Version2 || version == Version3 {
			n.DataSize = util.BytesToUint32(buf[NeedleHeaderSize : NeedleHeaderSize+DataSizeSize])

			startOffset := offset + NeedleHeaderSize
			if size.IsValid() {
				startOffset = offset + NeedleHeaderSize + DataSizeSize + int64(n.DataSize)
			}
			dataSize := GetActualSize(size, version)
			stopOffset := offset + dataSize
			metaFieldsLen := stopOffset - startOffset

			metaFieldsBuf := make([]byte, metaFieldsLen)
			count, err = r.ReadAt(metaFieldsBuf, startOffset)
			if err == io.EOF && int64(count) == metaFieldsLen {
				err = nil
			}
			if count <= 0 || err != nil {
				return err
			}

			var index int
			if size.IsValid() {
				index, err = n.readNeedleDataVersion2NonData(metaFieldsBuf)
				if err != nil {
					return err
				}
			}

			n.Checksum = CRC(util.BytesToUint32(metaFieldsBuf[index : index+NeedleChecksumSize]))
			if version == Version3 {
				n.AppendAtNs = util.BytesToUint64(metaFieldsBuf[index+NeedleChecksumSize : index+NeedleChecksumSize+TimestampSize])
			}
			return nil
		}
		// For v1, just skip Data
		return nil
	}
	// Otherwise, read the full on-disk entry size
	readLen := int(GetActualSize(size, version))
	bytes := make([]byte, readLen)
	count, err := r.ReadAt(bytes, offset)
	if err == io.EOF && count == readLen {
		err = nil
	}
	if count != readLen || err != nil {
		return err
	}
	return n.ReadBytes(bytes, offset, size, version)
}
