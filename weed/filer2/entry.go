package filer2

import (
	"os"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
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
	Md5           []byte
}

func (attr Attr) IsDirectory() bool {
	return attr.Mode&os.ModeDir > 0
}

type Entry struct {
	util.FullPath

	Attr
	Extended map[string][]byte

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
		Name:        entry.FullPath.Name(),
		IsDirectory: entry.IsDirectory(),
		Attributes:  EntryAttributeToPb(entry),
		Chunks:      entry.Chunks,
		Extended:    entry.Extended,
	}
}

func (entry *Entry) ToProtoFullEntry() *filer_pb.FullEntry {
	if entry == nil {
		return nil
	}
	dir, _ := entry.FullPath.DirAndName()
	return &filer_pb.FullEntry{
		Dir:   dir,
		Entry: entry.ToProtoEntry(),
	}
}

func FromPbEntry(dir string, entry *filer_pb.Entry) *Entry {
	return &Entry{
		FullPath: util.NewFullPath(dir, entry.Name),
		Attr:     PbToEntryAttribute(entry.Attributes),
		Chunks:   entry.Chunks,
	}
}
