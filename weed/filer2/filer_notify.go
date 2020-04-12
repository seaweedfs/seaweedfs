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

	event := &filer_pb.FullEventNotification{
		Directory:         dir,
		EventNotification: eventNotification,
	}
	data, err := proto.Marshal(event)
	if err != nil {
		glog.Errorf("failed to marshal filer_pb.FullEventNotification %+v: %v", event, err)
		return
	}

	f.metaLogBuffer.AddToBuffer([]byte(dir), data)

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

func (f *Filer) ReadLogBuffer(lastReadTime time.Time, eachEventFn func(fullpath string, eventNotification *filer_pb.EventNotification) error) (newLastReadTime time.Time, err error) {

	var buf []byte
	newLastReadTime, buf = f.metaLogBuffer.ReadFromBuffer(lastReadTime)
	var processedTs int64

	for pos := 0; pos+4 < len(buf); {

		size := util.BytesToUint32(buf[pos : pos+4])
		entryData := buf[pos+4 : pos+4+int(size)]

		logEntry := &filer_pb.LogEntry{}
		err = proto.Unmarshal(entryData, logEntry)
		if err != nil {
			glog.Errorf("unexpected unmarshal filer_pb.LogEntry: %v", err)
			return lastReadTime, fmt.Errorf("unexpected unmarshal filer_pb.LogEntry: %v", err)
		}

		event := &filer_pb.FullEventNotification{}
		err = proto.Unmarshal(logEntry.Data, event)
		if err != nil {
			glog.Errorf("unexpected unmarshal filer_pb.FullEventNotification: %v", err)
			return lastReadTime, fmt.Errorf("unexpected unmarshal filer_pb.FullEventNotification: %v", err)
		}

		err = eachEventFn(event.Directory, event.EventNotification)

		processedTs = logEntry.TsNs

		if err != nil {
			newLastReadTime = time.Unix(0, processedTs)
			return
		}

		pos += 4 + int(size)

	}

	newLastReadTime = time.Unix(0, processedTs)
	return

}
