package filer

import (
	"os"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type Attr struct {
	Mtime         time.Time   // time of last modification
	Crtime        time.Time   // time of creation (OS X only)
	Mode          os.FileMode // file mode
	Uid           uint32      // owner uid
	Gid           uint32      // group gid
	Mime          string      // mime type
	TtlSec        int32       // ttl in seconds
	UserName      string
	GroupNames    []string
	SymlinkTarget string
	Md5           []byte
	FileSize      uint64
	Rdev          uint32
	Inode         uint64
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

	HardLinkId         HardLinkId
	HardLinkCounter    int32
	Content            []byte
	Remote             *filer_pb.RemoteEntry
	Quota              int64
	WORMEnforcedAtTsNs int64
}

func (entry *Entry) Size() uint64 {
	return maxUint64(maxUint64(TotalSize(entry.GetChunks()), entry.FileSize), uint64(len(entry.Content)))
}

func (entry *Entry) Timestamp() time.Time {
	if entry.IsDirectory() {
		return entry.Crtime
	} else {
		return entry.Mtime
	}
}

func (entry *Entry) ShallowClone() *Entry {
	if entry == nil {
		return nil
	}
	newEntry := &Entry{}
	newEntry.FullPath = entry.FullPath
	newEntry.Attr = entry.Attr
	newEntry.Chunks = entry.Chunks
	newEntry.Extended = entry.Extended
	newEntry.HardLinkId = entry.HardLinkId
	newEntry.HardLinkCounter = entry.HardLinkCounter
	newEntry.Content = entry.Content
	newEntry.Remote = entry.Remote
	newEntry.Quota = entry.Quota

	return newEntry
}

func (entry *Entry) ToProtoEntry() *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	message := &filer_pb.Entry{}
	message.Name = entry.FullPath.Name()
	entry.ToExistingProtoEntry(message)
	return message
}

func (entry *Entry) ToExistingProtoEntry(message *filer_pb.Entry) {
	if entry == nil {
		return
	}
	message.IsDirectory = entry.IsDirectory()
	message.Attributes = EntryAttributeToPb(entry)
	message.Chunks = entry.GetChunks()
	message.Extended = entry.Extended
	message.HardLinkId = entry.HardLinkId
	message.HardLinkCounter = entry.HardLinkCounter
	message.Content = entry.Content
	message.RemoteEntry = entry.Remote
	message.Quota = entry.Quota
	message.WormEnforcedAtTsNs = entry.WORMEnforcedAtTsNs
}

func FromPbEntryToExistingEntry(message *filer_pb.Entry, fsEntry *Entry) {
	fsEntry.Attr = PbToEntryAttribute(message.Attributes)
	fsEntry.Chunks = message.Chunks
	fsEntry.Extended = message.Extended
	fsEntry.HardLinkId = HardLinkId(message.HardLinkId)
	fsEntry.HardLinkCounter = message.HardLinkCounter
	fsEntry.Content = message.Content
	fsEntry.Remote = message.RemoteEntry
	fsEntry.Quota = message.Quota
	fsEntry.FileSize = FileSize(message)
	fsEntry.WORMEnforcedAtTsNs = message.WormEnforcedAtTsNs
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

func (entry *Entry) GetChunks() []*filer_pb.FileChunk {
	return entry.Chunks
}

func FromPbEntry(dir string, entry *filer_pb.Entry) *Entry {
	t := &Entry{}
	t.FullPath = util.NewFullPath(dir, entry.Name)
	FromPbEntryToExistingEntry(entry, t)
	return t
}

func maxUint64(x, y uint64) uint64 {
	if x > y {
		return x
	}
	return y
}
