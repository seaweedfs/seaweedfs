package replication

import (
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type Replicator struct {
	sink *sink.FilerSink
}

func NewReplicator(config util.Configuration) *Replicator {

	sink := &sink.FilerSink{}
	sink.Initialize(config)

	return &Replicator{
		sink: sink,
	}
}

func (r *Replicator) Replicate(key string, message *filer_pb.EventNotification) error {
	if message.OldEntry != nil && message.NewEntry == nil {
		return r.sink.DeleteEntry(message.OldEntry, message.DeleteChunks)
	}
	if message.OldEntry == nil && message.NewEntry != nil {
		return r.sink.CreateEntry(message.NewEntry)
	}
	return r.sink.UpdateEntry(message.OldEntry, message.NewEntry, message.DeleteChunks)
}
