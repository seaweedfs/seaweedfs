package filer2

import (
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type Attr struct {
	Mtime         time.Time   // time of last modification
	Crtime        time.Time   // time of creation (OS X only)
	Mode          os.FileMode // file mode
	Uid           uint32      // owner uid
	Gid           uint32      // group gid
	Mime          string      // mime type
	Replication   string      // replication
	Collection    string      // collection name
	TtlSec        int32       // ttl in seconds
	UserName      string
	GroupNames    []string
	SymlinkTarget string
}

func (attr Attr) IsDirectory() bool {
	return attr.Mode&os.ModeDir > 0
}

type Entry struct {
	FullPath

	Attr

	// the following is for files
	Chunks []*filer_pb.FileChunk `json:"chunks,omitempty"`
}

func (entry *Entry) Size() uint64 {
	return TotalSize(entry.Chunks)
}

func (entry *Entry) Timestamp() time.Time {
	if entry.IsDirectory() {
		return entry.Crtime
	} else {
		return entry.Mtime
	}
}

func (entry *Entry) ToProtoEntry() *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	return &filer_pb.Entry{
		Name:        string(entry.FullPath),
		IsDirectory: entry.IsDirectory(),
		Attributes:  EntryAttributeToPb(entry),
		Chunks:      entry.Chunks,
	}
}
