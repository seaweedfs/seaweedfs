package filer

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"google.golang.org/protobuf/proto"
)

func TestProtoMarshal(t *testing.T) {

	oldEntry := &Entry{
		FullPath: util.FullPath("/this/path/to"),
		Attr: Attr{
			Mtime:  time.Now(),
			Mode:   0644,
			Uid:    1,
			Mime:   "text/json",
			TtlSec: 25,
		},
		Chunks: []*filer_pb.FileChunk{
			{
				FileId:       "234,2423423422",
				Offset:       234234,
				Size:         234,
				ModifiedTsNs: 12312423,
				ETag:         "2342342354",
				SourceFileId: "23234,2342342342",
			},
		},
	}

	notification := &filer_pb.EventNotification{
		OldEntry:     oldEntry.ToProtoEntry(),
		NewEntry:     nil,
		DeleteChunks: true,
	}

	text, _ := proto.Marshal(notification)

	notification2 := &filer_pb.EventNotification{}
	proto.Unmarshal(text, notification2)

	if notification2.OldEntry.GetChunks()[0].SourceFileId != notification.OldEntry.GetChunks()[0].SourceFileId {
		t.Fatalf("marshal/unmarshal error: %s", text)
	}

	println(string(text))

}
