package filer

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (entry *Entry) EncodeAttributesAndChunks() ([]byte, error) {
	message := &filer_pb.Entry{}
	entry.ToExistingProtoEntry(message)
	return proto.Marshal(message)
}

func (entry *Entry) DecodeAttributesAndChunks(blob []byte) error {

	message := &filer_pb.Entry{}

	if err := proto.UnmarshalMerge(blob, message); err != nil {
		return fmt.Errorf("decoding value blob for %s: %v", entry.FullPath, err)
	}

	FromPbEntryToExistingEntry(message, entry)

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
		Collection:    entry.Attr.Collection,
		Replication:   entry.Attr.Replication,
		TtlSec:        entry.Attr.TtlSec,
		DiskType:      entry.Attr.DiskType,
		UserName:      entry.Attr.UserName,
		GroupName:     entry.Attr.GroupNames,
		SymlinkTarget: entry.Attr.SymlinkTarget,
		Md5:           entry.Attr.Md5,
		FileSize:      entry.Attr.FileSize,
		Rdev:          entry.Attr.Rdev,
		Inode:         entry.Attr.Inode,
	}
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
	t.Collection = attr.Collection
	t.Replication = attr.Replication
	t.TtlSec = attr.TtlSec
	t.DiskType = attr.DiskType
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
