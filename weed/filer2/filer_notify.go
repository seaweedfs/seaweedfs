package filer2

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/notification"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (f *Filer) NotifyUpdateEvent(oldEntry, newEntry *Entry, deleteChunks bool) {
	var key string
	if oldEntry != nil {
		key = string(oldEntry.FullPath)
	} else if newEntry != nil {
		key = string(newEntry.FullPath)
	} else {
		return
	}

	println("key:", key)

	if strings.HasPrefix(key, "/.meta") {
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
		glog.V(3).Infof("notifying entry update %v", key)
		notification.Queue.SendMessage(key, eventNotification)
	}

	f.logMetaEvent(time.Now(), key, eventNotification)

}

func (f *Filer) logMetaEvent(ts time.Time, dir string, eventNotification *filer_pb.EventNotification) {
	event := &filer_pb.FullEventNotification{
		Directory:         dir,
		EventNotification: eventNotification,
	}
	data, err := proto.Marshal(event)
	if err != nil {
		glog.Errorf("failed to marshal filer_pb.FullEventNotification %+v: %v", event, err)
		return
	}

	f.metaLogBuffer.AddToBuffer(ts, []byte(dir), data)

}

func (f *Filer) logFlushFunc(startTime, stopTime time.Time, buf []byte) {
	targetFile := fmt.Sprintf("/.meta/log/%04d/%02d/%02d/%02d/%02d/%02d-%02d.log",
		startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), startTime.Minute(),
		startTime.Second(), stopTime.Second())

	if err := f.appendToFile(targetFile, buf); err != nil {
		glog.V(0).Infof("log write failed %s: %v", targetFile, err)
	}
}

type LogBuffer struct {
	buf           []byte
	pos           int
	startTime     time.Time
	stopTime      time.Time
	sizeBuf       []byte
	flushInterval time.Duration
	flushFn       func(startTime, stopTime time.Time, buf []byte)
	sync.Mutex
}

func NewLogBuffer(flushInterval time.Duration, flushFn func(startTime, stopTime time.Time, buf []byte)) *LogBuffer {
	lb := &LogBuffer{
		buf:           make([]byte, 4*0124*1024),
		sizeBuf:       make([]byte, 4),
		flushInterval: 2 * time.Second, // flushInterval,
		flushFn:       flushFn,
	}
	go lb.loopFlush()
	return lb
}

func (m *LogBuffer) loopFlush() {
	for {
		m.Lock()
		m.flush()
		m.Unlock()
		time.Sleep(m.flushInterval)
	}
}

func (m *LogBuffer) flush() {
	if m.flushFn != nil && m.pos > 0 {
		m.flushFn(m.startTime, m.stopTime, m.buf[:m.pos])
		m.pos = 0
	}
}

func (m *LogBuffer) AddToBuffer(ts time.Time, key, data []byte) {

	logEntry := &filer_pb.LogEntry{
		TsNs:             ts.UnixNano(),
		PartitionKeyHash: util.HashToInt32(key),
		Data:             data,
	}

	logEntryData, _ := proto.Marshal(logEntry)

	size := len(logEntryData)

	m.Lock()
	defer m.Unlock()

	if m.pos == 0 {
		m.startTime = ts
	}

	if m.startTime.Add(m.flushInterval).Before(ts) || len(m.buf)-m.pos < size+4 {
		m.flush()
		m.startTime = ts
	}
	m.stopTime = ts

	util.Uint32toBytes(m.sizeBuf, uint32(size))
	copy(m.buf[m.pos:m.pos+4], m.sizeBuf)

	copy(m.buf[m.pos+4:m.pos+4+size], logEntryData)
	m.pos += size + 4
}
