package localincrementalsink

import (
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/sink/localsink"
)

type LocalIncSink struct {
	localsink.LocalSink
}

func (localincsink *LocalIncSink) GetName() string {
	return "local_incremental"
}

func init() {
	sink.Sinks = append(sink.Sinks, &LocalIncSink{})
}
