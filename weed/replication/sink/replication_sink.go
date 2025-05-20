package sink

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type ReplicationSink interface {
	GetName() string
	Initialize(configuration util.Configuration, prefix string) error
	DeleteEntry(ctx context.Context, key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error
	CreateEntry(ctx context.Context, key string, entry *filer_pb.Entry, signatures []int32) error
	UpdateEntry(ctx context.Context, key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error)
	GetSinkToDirectory() string
	SetSourceFiler(s *source.FilerSource)
	IsIncremental() bool
}

var (
	Sinks []ReplicationSink
)
