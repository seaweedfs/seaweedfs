package sink

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type ReplicationSink interface {
	GetName() string
	Initialize(configuration util.Configuration, prefix string) error
	DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error
	CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error
	UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error)
	GetSinkToDirectory() string
	// GetDestinationIdentity distinguishes this sink's write destination from
	// any other destination the same sink type could write to: endpoint or
	// account, bucket or container, and directory. filer.backup keys its resume
	// checkpoint on it, so two configurations writing to different places must
	// not share a value.
	GetDestinationIdentity() string
	SetSourceFiler(s *source.FilerSource)
	IsIncremental() bool
}

// EntryMover is an optional capability for sinks that can relocate an entry
// natively, in one atomic step, instead of create-then-delete. Drivers prefer
// it for a rename so a failed copy can never leave the source deleted with no
// committed destination, a directory move never deletes descendants before they
// are recreated, and the entry's chunks are neither re-copied nor leaked.
type EntryMover interface {
	MoveEntry(oldKey, newKey string, newEntry *filer_pb.Entry, signatures []int32) error
}

var (
	Sinks []ReplicationSink
)
