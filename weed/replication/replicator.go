package replication

import (
	"path/filepath"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
		"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

type Replicator struct {
	sink   sink.ReplicationSink
	source *source.FilerSource
}

func NewReplicator(sourceConfig util.Configuration, dataSink sink.ReplicationSink) *Replicator {

	source := &source.FilerSource{}
	source.Initialize(sourceConfig)

	dataSink.SetSourceFiler(source)

	return &Replicator{
		sink:   dataSink,
		source: source,
	}
}

func (r *Replicator) Replicate(key string, message *filer_pb.EventNotification) error {
	if !strings.HasPrefix(key, r.source.Dir) {
		return nil
	}
	key = filepath.Join(r.sink.GetSinkToDirectory(), key[len(r.source.Dir):])
	if message.OldEntry != nil && message.NewEntry == nil {
		return r.sink.DeleteEntry(key, message.OldEntry.IsDirectory, message.DeleteChunks)
	}
	if message.OldEntry == nil && message.NewEntry != nil {
		return r.sink.CreateEntry(key, message.NewEntry)
	}
	if message.OldEntry == nil && message.NewEntry == nil {
		glog.V(0).Infof("weird message %+v", message)
		return nil
	}

	foundExisting, err := r.sink.UpdateEntry(key, message.OldEntry, message.NewEntry, message.DeleteChunks)
	if foundExisting {
		return err
	}

	return r.sink.CreateEntry(key, message.NewEntry)
}
