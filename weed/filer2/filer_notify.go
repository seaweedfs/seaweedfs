package filer2

import (
	"github.com/chrislusf/seaweedfs/weed/notification"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

func (f *Filer) NotifyUpdateEvent(oldEntry, newEntry *Entry) {
	var key string
	if oldEntry != nil {
		key = string(oldEntry.FullPath)
	} else if newEntry != nil {
		key = string(newEntry.FullPath)
	} else {
		return
	}

	if notification.Queue != nil {

		glog.V(3).Infof("notifying entry update %v", key)

		notification.Queue.SendMessage(
			key,
			&filer_pb.EventNotification{
				OldEntry: toProtoEntry(oldEntry),
				NewEntry: toProtoEntry(newEntry),
			},
		)

	}

}

func toProtoEntry(entry *Entry) *filer_pb.Entry {
	if entry == nil {
		return nil
	}
	return &filer_pb.Entry{
		Name:        string(entry.FullPath),
		IsDirectory: entry.IsDirectory(),
		Attributes:  EntryAttributeToPb(entry),
		Chunks:      entry.Chunks,
	}
}
