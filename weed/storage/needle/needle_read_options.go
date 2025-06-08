package needle

import (
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// NeedleReadOptions specifies which parts of the Needle to read.
type NeedleReadOptions struct {
	ReadHeader bool // always true for any read
	ReadData   bool // read the Data field
	ReadMeta   bool // read metadata fields (Name, Mime, LastModified, Ttl, Pairs, etc.)
}

// ReadFromFile reads the Needle from the backend file according to the specified options.
// For now, this is equivalent to ReadData (reads everything).
func (n *Needle) ReadFromFile(r backend.BackendStorageFile, offset int64, size Size, version Version, opts NeedleReadOptions) error {
	// Always read header and body for now (full read)
	bytes, err := ReadNeedleBlob(r, offset, size, version)
	if err != nil {
		return err
	}
	return n.ReadBytes(bytes, offset, size, version)
}
