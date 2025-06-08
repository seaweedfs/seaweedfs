package needle

import (
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
func (n *Needle) ReadFromFile(r backend.BackendStorageFile, offset int64, size Size, version Version, opts NeedleReadOptions) error {
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
		// Read header first
		header := make([]byte, NeedleHeaderSize)
		count, err := r.ReadAt(header, offset)
		if err == io.EOF && count == NeedleHeaderSize {
			err = nil
		}
		if count != NeedleHeaderSize || err != nil {
			return err
		}
		n.ParseNeedleHeader(header)

		// Now read meta fields after DataSize+Data
		metaOffset := offset + int64(NeedleHeaderSize)
		metaIndex := 0
		if version == Version2 || version == Version3 {
			// Read DataSize to know how much to skip
			dsBuf := make([]byte, 4)
			count, err := r.ReadAt(dsBuf, metaOffset)
			if err == io.EOF && count == 4 {
				err = nil
			}
			if count != 4 || err != nil {
				return err
			}
			dataSize := int(util.BytesToUint32(dsBuf))
			metaIndex = 4 + dataSize
			// Read meta fields (Flags, Name, Mime, etc.)
			metaFieldsLen := int(n.Size) - dataSize // upper bound, may be more than needed
			metaFieldsBuf := make([]byte, metaFieldsLen)
			count, err = r.ReadAt(metaFieldsBuf, metaOffset+int64(metaIndex))
			if err == io.EOF && count == metaFieldsLen {
				err = nil
			}
			if count <= 0 || err != nil {
				return err
			}
			_, err = n.readNeedleDataVersion2NonData(metaFieldsBuf)
			if err != nil {
				return err
			}
			// Now read checksum and (for v3) appendAtNs at the end
			endMetaOffset := offset + int64(NeedleHeaderSize) + int64(n.Size)
			endMetaLen := NeedleChecksumSize
			if version == Version3 {
				endMetaLen += TimestampSize
			}
			endMetaBuf := make([]byte, endMetaLen)
			count, err = r.ReadAt(endMetaBuf, endMetaOffset)
			if err == io.EOF && count == endMetaLen {
				err = nil
			}
			if count != endMetaLen || err != nil {
				return err
			}
			n.Checksum = CRC(util.BytesToUint32(endMetaBuf[:NeedleChecksumSize]))
			if version == Version3 {
				n.AppendAtNs = util.BytesToUint64(endMetaBuf[NeedleChecksumSize : NeedleChecksumSize+TimestampSize])
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
