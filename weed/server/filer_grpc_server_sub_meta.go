package weed_server

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/stats"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

const (
	// MaxUnsyncedEvents send empty notification with timestamp when certain amount of events have been filtered
	MaxUnsyncedEvents = 1e3
)

func (fs *FilerServer) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer) error {

	peerAddress := findClientAddress(stream.Context(), 0)

	isReplacing, alreadyKnown, clientName := fs.addClient("", req.ClientName, peerAddress, req.ClientId, req.ClientEpoch)
	if isReplacing {
		fs.filer.MetaAggregator.ListenersCond.Broadcast() // nudges the subscribers that are waiting
	} else if alreadyKnown {
		fs.filer.MetaAggregator.ListenersCond.Broadcast() // nudges the subscribers that are waiting
		return fmt.Errorf("duplicated subscription detected for client %s id %d", clientName, req.ClientId)
	}
	defer func() {
		glog.V(0).Infof("disconnect %v subscriber %s clientId:%d", clientName, req.PathPrefix, req.ClientId)
		fs.deleteClient("", clientName, req.ClientId, req.ClientEpoch)
		fs.filer.MetaAggregator.ListenersCond.Broadcast() // nudges the subscribers that are waiting
	}()

	lastReadTime := log_buffer.NewMessagePosition(req.SinceNs, -2)
	glog.V(0).Infof(" %v starts to subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

	eachEventNotificationFn := fs.eachEventNotificationFn(req, stream, clientName)

	eachLogEntryFn := eachLogEntryFn(eachEventNotificationFn)

	var processedTsNs int64
	var readPersistedLogErr error
	var readInMemoryLogErr error
	var isDone bool

	for {

		glog.V(4).Infof("read on disk %v aggregated subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		processedTsNs, isDone, readPersistedLogErr = fs.filer.ReadPersistedLogBuffer(lastReadTime, req.UntilNs, eachLogEntryFn)
		if readPersistedLogErr != nil {
			return fmt.Errorf("reading from persisted logs: %v", readPersistedLogErr)
		}
		if isDone {
			return nil
		}

		glog.V(4).Infof("processed to %v: %v", clientName, processedTsNs)
		if processedTsNs != 0 {
			lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
		} else {
			nextDayTs := util.GetNextDayTsNano(lastReadTime.UnixNano())
			position := log_buffer.NewMessagePosition(nextDayTs, -2)
			found, err := fs.filer.HasPersistedLogFiles(position)
			if err != nil {
				return fmt.Errorf("checking persisted log files: %v", err)
			}
			if found {
				lastReadTime = position
			}
		}

		glog.V(4).Infof("read in memory %v aggregated subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		lastReadTime, isDone, readInMemoryLogErr = fs.filer.MetaAggregator.MetaLogBuffer.LoopProcessLogData("aggMeta:"+clientName, lastReadTime, req.UntilNs, func() bool {
			fs.filer.MetaAggregator.ListenersLock.Lock()
			fs.filer.MetaAggregator.ListenersCond.Wait()
			fs.filer.MetaAggregator.ListenersLock.Unlock()
			return fs.hasClient(req.ClientId, req.ClientEpoch)
		}, eachLogEntryFn)
		if readInMemoryLogErr != nil {
			if readInMemoryLogErr == log_buffer.ResumeFromDiskError {
				continue
			}
			glog.Errorf("processed to %v: %v", lastReadTime, readInMemoryLogErr)
			if readInMemoryLogErr != log_buffer.ResumeError {
				break
			}
		}
		if isDone {
			return nil
		}
		if !fs.hasClient(req.ClientId, req.ClientEpoch) {
			glog.V(0).Infof("client %v is closed", clientName)
			return nil
		}

		time.Sleep(1127 * time.Millisecond)
	}

	return readInMemoryLogErr

}

func (fs *FilerServer) SubscribeLocalMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeLocalMetadataServer) error {

	peerAddress := findClientAddress(stream.Context(), 0)

	// use negative client id to differentiate from addClient()/deleteClient() used in SubscribeMetadata()
	req.ClientId = -req.ClientId

	isReplacing, alreadyKnown, clientName := fs.addClient("local", req.ClientName, peerAddress, req.ClientId, req.ClientEpoch)
	if isReplacing {
		fs.listenersCond.Broadcast() // nudges the subscribers that are waiting
	} else if alreadyKnown {
		return fmt.Errorf("duplicated local subscription detected for client %s clientId:%d", clientName, req.ClientId)
	}
	defer func() {
		glog.V(0).Infof("disconnect %v local subscriber %s clientId:%d", clientName, req.PathPrefix, req.ClientId)
		fs.deleteClient("local", clientName, req.ClientId, req.ClientEpoch)
		fs.listenersCond.Broadcast() // nudges the subscribers that are waiting
	}()

	lastReadTime := log_buffer.NewMessagePosition(req.SinceNs, -2)
	glog.V(0).Infof(" + %v local subscribe %s from %+v clientId:%d", clientName, req.PathPrefix, lastReadTime, req.ClientId)

	eachEventNotificationFn := fs.eachEventNotificationFn(req, stream, clientName)

	eachLogEntryFn := eachLogEntryFn(eachEventNotificationFn)

	var processedTsNs int64
	var readPersistedLogErr error
	var readInMemoryLogErr error
	var isDone bool

	for {
		// println("reading from persisted logs ...")
		glog.V(0).Infof("read on disk %v local subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)
		processedTsNs, isDone, readPersistedLogErr = fs.filer.ReadPersistedLogBuffer(lastReadTime, req.UntilNs, eachLogEntryFn)
		if readPersistedLogErr != nil {
			glog.V(0).Infof("read on disk %v local subscribe %s from %+v: %v", clientName, req.PathPrefix, lastReadTime, readPersistedLogErr)
			return fmt.Errorf("reading from persisted logs: %v", readPersistedLogErr)
		}
		if isDone {
			return nil
		}

		if processedTsNs != 0 {
			lastReadTime = log_buffer.NewMessagePosition(processedTsNs, -2)
		} else {
			if readInMemoryLogErr == log_buffer.ResumeFromDiskError {
				time.Sleep(1127 * time.Millisecond)
				continue
			}
		}

		glog.V(0).Infof("read in memory %v local subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

		lastReadTime, isDone, readInMemoryLogErr = fs.filer.LocalMetaLogBuffer.LoopProcessLogData("localMeta:"+clientName, lastReadTime, req.UntilNs, func() bool {
			fs.listenersLock.Lock()
			atomic.AddInt64(&fs.listenersWaits, 1)
			fs.listenersCond.Wait()
			atomic.AddInt64(&fs.listenersWaits, -1)
			fs.listenersLock.Unlock()
			if !fs.hasClient(req.ClientId, req.ClientEpoch) {
				return false
			}
			return true
		}, eachLogEntryFn)
		if readInMemoryLogErr != nil {
			if readInMemoryLogErr == log_buffer.ResumeFromDiskError {
				continue
			}
			glog.Errorf("processed to %v: %v", lastReadTime, readInMemoryLogErr)
			if readInMemoryLogErr != log_buffer.ResumeError {
				break
			}
		}
		if isDone {
			return nil
		}
		if !fs.hasClient(req.ClientId, req.ClientEpoch) {
			return nil
		}
	}

	return readInMemoryLogErr

}

func eachLogEntryFn(eachEventNotificationFn func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error) log_buffer.EachLogEntryFuncType {
	return func(logEntry *filer_pb.LogEntry) (bool, error) {
		event := &filer_pb.SubscribeMetadataResponse{}
		if err := proto.Unmarshal(logEntry.Data, event); err != nil {
			glog.Errorf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %v", err)
			return false, fmt.Errorf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %v", err)
		}

		if err := eachEventNotificationFn(event.Directory, event.EventNotification, event.TsNs); err != nil {
			return false, err
		}

		return false, nil
	}
}

func (fs *FilerServer) eachEventNotificationFn(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer, clientName string) func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
	filtered := 0

	return func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
		defer func() {
			if filtered > MaxUnsyncedEvents {
				if err := stream.Send(&filer_pb.SubscribeMetadataResponse{
					EventNotification: &filer_pb.EventNotification{},
					TsNs:              tsNs,
				}); err == nil {
					filtered = 0
				}
			}
		}()

		filtered++
		foundSelf := false
		for _, sig := range eventNotification.Signatures {
			if sig == req.Signature && req.Signature != 0 {
				return nil
			}
			if sig == fs.filer.Signature {
				foundSelf = true
			}
		}
		if !foundSelf {
			eventNotification.Signatures = append(eventNotification.Signatures, fs.filer.Signature)
		}

		// get complete path to the file or directory
		var entryName string
		if eventNotification.OldEntry != nil {
			entryName = eventNotification.OldEntry.Name
		} else if eventNotification.NewEntry != nil {
			entryName = eventNotification.NewEntry.Name
		}

		fullpath := util.Join(dirPath, entryName)

		// skip on filer internal meta logs
		if strings.HasPrefix(fullpath, filer.SystemLogDir) {
			return nil
		}

		if hasPrefixIn(fullpath, req.PathPrefixes) {
			// good
		} else if matchByDirectory(dirPath, req.Directories) {
			// good
		} else {
			if !strings.HasPrefix(fullpath, req.PathPrefix) {
				if eventNotification.NewParentPath != "" {
					newFullPath := util.Join(eventNotification.NewParentPath, entryName)
					if !strings.HasPrefix(newFullPath, req.PathPrefix) {
						return nil
					}
				} else {
					return nil
				}
			}
		}

		// collect timestamps for path
		stats.FilerServerLastSendTsOfSubscribeGauge.WithLabelValues(fs.option.Host.String(), req.ClientName, req.PathPrefix).Set(float64(tsNs))

		message := &filer_pb.SubscribeMetadataResponse{
			Directory:         dirPath,
			EventNotification: eventNotification,
			TsNs:              tsNs,
		}
		// println("sending", dirPath, entryName)
		if err := stream.Send(message); err != nil {
			glog.V(0).Infof("=> client %v: %+v", clientName, err)
			return err
		}
		filtered = 0
		return nil
	}
}

func hasPrefixIn(text string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(text, p) {
			return true
		}
	}
	return false
}

func matchByDirectory(dirPath string, directories []string) bool {
	for _, dir := range directories {
		if dirPath == dir {
			return true
		}
	}
	return false
}

func (fs *FilerServer) addClient(prefix string, clientType string, clientAddress string, clientId int32, clientEpoch int32) (isReplacing, alreadyKnown bool, clientName string) {
	clientName = clientType + "@" + clientAddress
	glog.V(0).Infof("+ %v listener %v clientId %v clientEpoch %v", prefix, clientName, clientId, clientEpoch)
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()
		epoch, found := fs.knownListeners[clientId]
		if !found || epoch < clientEpoch {
			fs.knownListeners[clientId] = clientEpoch
			isReplacing = true
		} else {
			alreadyKnown = true
		}
	}
	return
}

func (fs *FilerServer) deleteClient(prefix string, clientName string, clientId int32, clientEpoch int32) {
	glog.V(0).Infof("- %v listener %v clientId %v clientEpoch %v", prefix, clientName, clientId, clientEpoch)
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()
		epoch, found := fs.knownListeners[clientId]
		if found && epoch <= clientEpoch {
			delete(fs.knownListeners, clientId)
		}
	}
}

func (fs *FilerServer) hasClient(clientId int32, clientEpoch int32) bool {
	if clientId != 0 {
		fs.knownListenersLock.Lock()
		defer fs.knownListenersLock.Unlock()
		epoch, found := fs.knownListeners[clientId]
		if found && epoch <= clientEpoch {
			return true
		}
	}
	return false
}
