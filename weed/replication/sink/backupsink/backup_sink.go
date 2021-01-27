package backupsink

import (
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/sink/localsink"
)

type BackupSink struct {
	localsink.LocalSink
}

func (backupsink *BackupSink) GetName() string {
	return "backup"
}

func init() {
	sink.Sinks = append(sink.Sinks, &BackupSink{})
}
