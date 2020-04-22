package filer2

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/notification"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (f *Filer) NotifyUpdateEvent(oldEntry, newEntry *Entry, deleteChunks bool) {
	var fullpath string
	if oldEntry != nil {
		fullpath = string(oldEntry.FullPath)
	} else if newEntry != nil {
		fullpath = string(newEntry.FullPath)
	} else {
		return
	}

	// println("fullpath:", fullpath)

	if strings.HasPrefix(fullpath, SystemLogDir) {
		return
	}

	newParentPath := ""
	if newEntry != nil {
		newParentPath, _ = newEntry.FullPath.DirAndName()
	}
	eventNotification := &filer_pb.EventNotification{
		OldEntry:      oldEntry.ToProtoEntry(),
		NewEntry:      newEntry.ToProtoEntry(),
		DeleteChunks:  deleteChunks,
		NewParentPath: newParentPath,
	}

	if notification.Queue != nil {
		glog.V(3).Infof("notifying entry update %v", fullpath)
		notification.Queue.SendMessage(fullpath, eventNotification)
	}

	f.logMetaEvent(fullpath, eventNotification)

}

func (f *Filer) logMetaEvent(fullpath string, eventNotification *filer_pb.EventNotification) {

	dir, _ := util.FullPath(fullpath).DirAndName()

	event := &filer_pb.SubscribeMetadataResponse{
		Directory:         dir,
		EventNotification: eventNotification,
		TsNs:              time.Now().UnixNano(),
	}
	data, err := proto.Marshal(event)
	if err != nil {
		glog.Errorf("failed to marshal filer_pb.SubscribeMetadataResponse %+v: %v", event, err)
		return
	}

	f.MetaLogBuffer.AddToBuffer([]byte(dir), data)

}

func (f *Filer) logFlushFunc(startTime, stopTime time.Time, buf []byte) {

	targetFile := fmt.Sprintf("%s/%04d-%02d-%02d/%02d-%02d.segment", SystemLogDir,
		startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), startTime.Minute(),
		// startTime.Second(), startTime.Nanosecond(),
	)

	if err := f.appendToFile(targetFile, buf); err != nil {
		glog.V(0).Infof("log write failed %s: %v", targetFile, err)
	}
}
