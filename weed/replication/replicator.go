package replication

import (
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type Replicator struct {
	sink   sink.ReplicationSink
	source *source.FilerSource
}

func NewReplicator(sourceConfig, sinkConfig util.Configuration) *Replicator {

	sink := &sink.FilerSink{}
	sink.Initialize(sinkConfig)

	source := &source.FilerSource{}
	source.Initialize(sourceConfig)

	if sourceConfig.GetString("grpcAddress") == sinkConfig.GetString("grpcAddress") {
		fromDir := sourceConfig.GetString("directory")
		toDir := sinkConfig.GetString("directory")
		if strings.HasPrefix(toDir, fromDir) {
			glog.Fatalf("recursive replication! source directory %s includes the sink directory %s", fromDir, toDir)
		}
	}

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
	key = r.sink.GetSinkToDirectory() + key[len(r.source.Dir):]
	if message.OldEntry != nil && message.NewEntry == nil {
		return r.sink.DeleteEntry(key, message.OldEntry, message.DeleteChunks)
	}
	if message.OldEntry == nil && message.NewEntry != nil {
		return r.sink.CreateEntry(key, message.NewEntry)
	}
	return r.sink.UpdateEntry(key, message.OldEntry, message.NewEntry, message.DeleteChunks)
}
