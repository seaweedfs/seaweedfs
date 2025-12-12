package filer

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

// pbEntryPool reduces allocations in EncodeAttributesAndChunks and DecodeAttributesAndChunks
// which are called on every filer store operation
var pbEntryPool = sync.Pool{
	New: func() interface{} {
		return &filer_pb.Entry{
			Attributes: &filer_pb.FuseAttributes{}, // Pre-allocate attributes
		}
	},
}

// resetPbEntry clears a protobuf Entry for reuse
func resetPbEntry(e *filer_pb.Entry) {
	e.Name = ""
	e.IsDirectory = false
	// Reset attributes in place instead of nilling
	if e.Attributes != nil {
		resetFuseAttributes(e.Attributes)
	}
	e.Chunks = nil
	e.Extended = nil
	e.HardLinkId = nil
	e.HardLinkCounter = 0
	e.Content = nil
	e.RemoteEntry = nil
	e.Quota = 0
	e.WormEnforcedAtTsNs = 0
}

// resetFuseAttributes clears FuseAttributes for reuse
func resetFuseAttributes(a *filer_pb.FuseAttributes) {
	a.Crtime = 0
	a.Mtime = 0
	a.FileMode = 0
	a.Uid = 0
	a.Gid = 0
	a.Mime = ""
	a.TtlSec = 0
	a.UserName = ""
	a.GroupName = nil
	a.SymlinkTarget = ""
	a.Md5 = nil
	a.FileSize = 0
	a.Rdev = 0
	a.Inode = 0
}

func (entry *Entry) EncodeAttributesAndChunks() ([]byte, error) {
	message := pbEntryPool.Get().(*filer_pb.Entry)
	resetPbEntry(message)
	entry.ToExistingProtoEntry(message)
	data, err := proto.Marshal(message)
	// Clear references before returning to pool
	resetPbEntry(message)
	pbEntryPool.Put(message)
	return data, err
}

func (entry *Entry) DecodeAttributesAndChunks(blob []byte) error {
	message := pbEntryPool.Get().(*filer_pb.Entry)
	resetPbEntry(message)

	if err := proto.Unmarshal(blob, message); err != nil {
		pbEntryPool.Put(message)
		return fmt.Errorf("decoding value blob for %s: %v", entry.FullPath, err)
	}

	FromPbEntryToExistingEntry(message, entry)

	// Clear references before returning to pool
	resetPbEntry(message)
	pbEntryPool.Put(message)

	return nil
}

func EntryAttributeToPb(entry *Entry) *filer_pb.FuseAttributes {

	return &filer_pb.FuseAttributes{
		Crtime:        entry.Attr.Crtime.Unix(),
		Mtime:         entry.Attr.Mtime.Unix(),
		FileMode:      uint32(entry.Attr.Mode),
		Uid:           entry.Uid,
		Gid:           entry.Gid,
		Mime:          entry.Mime,
		TtlSec:        entry.Attr.TtlSec,
		UserName:      entry.Attr.UserName,
		GroupName:     entry.Attr.GroupNames,
		SymlinkTarget: entry.Attr.SymlinkTarget,
		Md5:           entry.Attr.Md5,
		FileSize:      entry.Attr.FileSize,
		Rdev:          entry.Attr.Rdev,
		Inode:         entry.Attr.Inode,
	}
}

// EntryAttributeToExistingPb fills an existing FuseAttributes to avoid allocation
func EntryAttributeToExistingPb(entry *Entry, attr *filer_pb.FuseAttributes) {
	attr.Crtime = entry.Attr.Crtime.Unix()
	attr.Mtime = entry.Attr.Mtime.Unix()
	attr.FileMode = uint32(entry.Attr.Mode)
	attr.Uid = entry.Uid
	attr.Gid = entry.Gid
	attr.Mime = entry.Mime
	attr.TtlSec = entry.Attr.TtlSec
	attr.UserName = entry.Attr.UserName
	attr.GroupName = entry.Attr.GroupNames
	attr.SymlinkTarget = entry.Attr.SymlinkTarget
	attr.Md5 = entry.Attr.Md5
	attr.FileSize = entry.Attr.FileSize
	attr.Rdev = entry.Attr.Rdev
	attr.Inode = entry.Attr.Inode
}

func PbToEntryAttribute(attr *filer_pb.FuseAttributes) Attr {

	t := Attr{}

	if attr == nil {
		return t
	}

	t.Crtime = time.Unix(attr.Crtime, 0)
	t.Mtime = time.Unix(attr.Mtime, 0)
	t.Mode = os.FileMode(attr.FileMode)
	t.Uid = attr.Uid
	t.Gid = attr.Gid
	t.Mime = attr.Mime
	t.TtlSec = attr.TtlSec
	t.UserName = attr.UserName
	t.GroupNames = attr.GroupName
	t.SymlinkTarget = attr.SymlinkTarget
	t.Md5 = attr.Md5
	t.FileSize = attr.FileSize
	t.Rdev = attr.Rdev
	t.Inode = attr.Inode

	return t
}

func EqualEntry(a, b *Entry) bool {
	if a == b {
		return true
	}
	if a == nil && b != nil || a != nil && b == nil {
		return false
	}
	if !proto.Equal(EntryAttributeToPb(a), EntryAttributeToPb(b)) {
		return false
	}
	if len(a.Chunks) != len(b.Chunks) {
		return false
	}

	if !eq(a.Extended, b.Extended) {
		return false
	}

	if !bytes.Equal(a.Md5, b.Md5) {
		return false
	}

	for i := 0; i < len(a.Chunks); i++ {
		if !proto.Equal(a.Chunks[i], b.Chunks[i]) {
			return false
		}
	}

	if !bytes.Equal(a.HardLinkId, b.HardLinkId) {
		return false
	}
	if a.HardLinkCounter != b.HardLinkCounter {
		return false
	}
	if !bytes.Equal(a.Content, b.Content) {
		return false
	}
	if !proto.Equal(a.Remote, b.Remote) {
		return false
	}
	if a.Quota != b.Quota {
		return false
	}
	return true
}

func eq(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if w, ok := b[k]; !ok || !bytes.Equal(v, w) {
			return false
		}
	}

	return true
}
