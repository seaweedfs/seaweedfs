package replication

import (
	"path/filepath"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/sink/filersink"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type Replicator struct {
	sink   sink.ReplicationSink
	source *source.FilerSource
}

func NewReplicator(sourceConfig, sinkConfig util.Configuration) *Replicator {

	sink := &filersink.FilerSink{}
	sink.Initialize(sinkConfig)

	source := &source.FilerSource{}
	source.Initialize(sourceConfig)

	sink.SetSourceFiler(source)

	return &Replicator{
		sink:   sink,
		source: source,
	}
}

func (r *Replicator) Replicate(key string, message *filer_pb.EventNotification) error {
	if !strings.HasPrefix(key, r.source.Dir) {
		return nil
	}
	key = filepath.Join(r.sink.GetSinkToDirectory(), key[len(r.source.Dir):])
	if message.OldEntry != nil && message.NewEntry == nil {
		return r.sink.DeleteEntry(key, message.OldEntry, message.DeleteChunks)
	}
	if message.OldEntry == nil && message.NewEntry != nil {
		return r.sink.CreateEntry(key, message.NewEntry)
	}
	return r.sink.UpdateEntry(key, message.OldEntry, message.NewEntry, message.DeleteChunks)
}
