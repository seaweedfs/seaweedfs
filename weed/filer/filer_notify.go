package filer

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (f *Filer) NotifyUpdateEvent(ctx context.Context, oldEntry, newEntry *Entry, deleteChunks, isFromOtherCluster bool, signatures []int32) {
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
	foundSelf := false
	for _, sig := range signatures {
		if sig == f.Signature {
			foundSelf = true
		}
	}
	if !foundSelf {
		signatures = append(signatures, f.Signature)
	}

	newParentPath := ""
	if newEntry != nil {
		newParentPath, _ = newEntry.FullPath.DirAndName()
	}
	eventNotification := &filer_pb.EventNotification{
		OldEntry:           oldEntry.ToProtoEntry(),
		NewEntry:           newEntry.ToProtoEntry(),
		DeleteChunks:       deleteChunks,
		NewParentPath:      newParentPath,
		IsFromOtherCluster: isFromOtherCluster,
		Signatures:         signatures,
	}

	if notification.Queue != nil {
		glog.V(3).Infof("notifying entry update %v", fullpath)
		if err := notification.Queue.SendMessage(fullpath, eventNotification); err != nil {
			// throw message
			glog.Error(err)
		}
	}

	f.logMetaEvent(ctx, fullpath, eventNotification)

}

func (f *Filer) logMetaEvent(ctx context.Context, fullpath string, eventNotification *filer_pb.EventNotification) {

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

	f.LocalMetaLogBuffer.AddToBuffer([]byte(dir), data, event.TsNs)

}

func (f *Filer) logFlushFunc(startTime, stopTime time.Time, buf []byte) {

	if len(buf) == 0 {
		return
	}

	startTime, stopTime = startTime.UTC(), stopTime.UTC()

	targetFile := fmt.Sprintf("%s/%04d-%02d-%02d/%02d-%02d.%08x", SystemLogDir,
		startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), startTime.Minute(), f.UniqueFilerId,
		// startTime.Second(), startTime.Nanosecond(),
	)

	for {
		if err := f.appendToFile(targetFile, buf); err != nil {
			glog.V(0).Infof("metadata log write failed %s: %v", targetFile, err)
			time.Sleep(737 * time.Millisecond)
		} else {
			break
		}
	}
}

func (f *Filer) ReadPersistedLogBuffer(startTime time.Time, stopTsNs int64, eachLogEntryFn func(logEntry *filer_pb.LogEntry) error) (lastTsNs int64, isDone bool, err error) {

	startTime = startTime.UTC()
	startDate := fmt.Sprintf("%04d-%02d-%02d", startTime.Year(), startTime.Month(), startTime.Day())
	startHourMinute := fmt.Sprintf("%02d-%02d", startTime.Hour(), startTime.Minute())
	var stopDate, stopHourMinute string
	if stopTsNs != 0 {
		stopTime := time.Unix(0, stopTsNs+24*60*60*int64(time.Nanosecond)).UTC()
		stopDate = fmt.Sprintf("%04d-%02d-%02d", stopTime.Year(), stopTime.Month(), stopTime.Day())
		stopHourMinute = fmt.Sprintf("%02d-%02d", stopTime.Hour(), stopTime.Minute())
	}

	sizeBuf := make([]byte, 4)
	startTsNs := startTime.UnixNano()

	dayEntries, _, listDayErr := f.ListDirectoryEntries(context.Background(), SystemLogDir, startDate, true, math.MaxInt32, "", "", "")
	if listDayErr != nil {
		return lastTsNs, isDone, fmt.Errorf("fail to list log by day: %v", listDayErr)
	}
	for _, dayEntry := range dayEntries {
		if stopDate != "" {
			if strings.Compare(dayEntry.Name(), stopDate) > 0 {
				break
			}
		}
		// println("checking day", dayEntry.FullPath)
		hourMinuteEntries, _, listHourMinuteErr := f.ListDirectoryEntries(context.Background(), util.NewFullPath(SystemLogDir, dayEntry.Name()), "", false, math.MaxInt32, "", "", "")
		if listHourMinuteErr != nil {
			return lastTsNs, isDone, fmt.Errorf("fail to list log %s by day: %v", dayEntry.Name(), listHourMinuteErr)
		}
		for _, hourMinuteEntry := range hourMinuteEntries {
			// println("checking hh-mm", hourMinuteEntry.FullPath)
			if dayEntry.Name() == startDate {
				hourMinute := util.FileNameBase(hourMinuteEntry.Name())
				if strings.Compare(hourMinute, startHourMinute) < 0 {
					continue
				}
			}
			if dayEntry.Name() == stopDate {
				hourMinute := util.FileNameBase(hourMinuteEntry.Name())
				if strings.Compare(hourMinute, stopHourMinute) > 0 {
					break
				}
			}
			// println("processing", hourMinuteEntry.FullPath)
			chunkedFileReader := NewChunkStreamReaderFromFiler(f.MasterClient, hourMinuteEntry.Chunks)
			if lastTsNs, err = ReadEachLogEntry(chunkedFileReader, sizeBuf, startTsNs, stopTsNs, eachLogEntryFn); err != nil {
				chunkedFileReader.Close()
				if err == io.EOF {
					continue
				}
				return lastTsNs, isDone, fmt.Errorf("reading %s: %v", hourMinuteEntry.FullPath, err)
			}
			chunkedFileReader.Close()
		}
	}

	return lastTsNs, isDone, nil
}

func ReadEachLogEntry(r io.Reader, sizeBuf []byte, startTsNs, stopTsNs int64, eachLogEntryFn func(logEntry *filer_pb.LogEntry) error) (lastTsNs int64, err error) {
	for {
		n, err := r.Read(sizeBuf)
		if err != nil {
			return lastTsNs, err
		}
		if n != 4 {
			return lastTsNs, fmt.Errorf("size %d bytes, expected 4 bytes", n)
		}
		size := util.BytesToUint32(sizeBuf)
		// println("entry size", size)
		entryData := make([]byte, size)
		n, err = r.Read(entryData)
		if err != nil {
			return lastTsNs, err
		}
		if n != int(size) {
			return lastTsNs, fmt.Errorf("entry data %d bytes, expected %d bytes", n, size)
		}
		logEntry := &filer_pb.LogEntry{}
		if err = proto.Unmarshal(entryData, logEntry); err != nil {
			return lastTsNs, err
		}
		if logEntry.TsNs <= startTsNs {
			continue
		}
		if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
			return lastTsNs, err
		}
		// println("each log: ", logEntry.TsNs)
		if err := eachLogEntryFn(logEntry); err != nil {
			return lastTsNs, err
		} else {
			lastTsNs = logEntry.TsNs
		}
	}
}
