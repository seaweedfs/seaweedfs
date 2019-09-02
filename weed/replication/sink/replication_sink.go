package sink

import (
	"context"
	"github.com/joeslay/seaweedfs/weed/pb/filer_pb"
	"github.com/joeslay/seaweedfs/weed/replication/source"
	"github.com/joeslay/seaweedfs/weed/util"
)

type ReplicationSink interface {
	GetName() string
	Initialize(configuration util.Configuration) error
	DeleteEntry(ctx context.Context, key string, isDirectory, deleteIncludeChunks bool) error
	CreateEntry(ctx context.Context, key string, entry *filer_pb.Entry) error
	UpdateEntry(ctx context.Context, key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool) (foundExistingEntry bool, err error)
	GetSinkToDirectory() string
	SetSourceFiler(s *source.FilerSource)
}

var (
	Sinks []ReplicationSink
)
