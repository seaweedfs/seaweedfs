package filer2

import (
	"os"
	"time"

	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/gogo/protobuf/proto"
)

func (entry *Entry) EncodeAttributesAndChunks() ([]byte, error) {
	message := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{
			Crtime:   entry.Attr.Crtime.Unix(),
			Mtime:    entry.Attr.Mtime.Unix(),
			FileMode: uint32(entry.Attr.Mode),
			Uid:      entry.Uid,
			Gid:      entry.Gid,
			Mime:     entry.Mime,
		},
		Chunks: entry.Chunks,
	}
	return proto.Marshal(message)
}

func (entry *Entry) DecodeAttributesAndChunks(blob []byte) error {

	message := &filer_pb.Entry{}

	if err := proto.UnmarshalMerge(blob, message); err != nil {
		return fmt.Errorf("decoding value blob for %s: %v", entry.FullPath, err)
	}

	entry.Attr.Crtime = time.Unix(message.Attributes.Crtime, 0)
	entry.Attr.Mtime = time.Unix(message.Attributes.Mtime, 0)
	entry.Attr.Mode = os.FileMode(message.Attributes.FileMode)
	entry.Attr.Uid = message.Attributes.Uid
	entry.Attr.Gid = message.Attributes.Gid
	entry.Attr.Mime = message.Attributes.Mime

	entry.Chunks = message.Chunks

	return nil
}
