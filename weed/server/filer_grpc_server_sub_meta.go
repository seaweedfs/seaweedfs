package weed_server

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer) error {

	peerAddress := findClientAddress(stream.Context(), 0)

	clientName := fs.addClient(req.ClientName, peerAddress)

	defer fs.deleteClient(clientName)

	lastReadTime := time.Unix(0, req.SinceNs)
	glog.V(0).Infof(" %v starts to subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

	eachEventNotificationFn := eachEventNotificationFn(req, stream, clientName)

	eachLogEntryFn := eachLogEntryFn(eachEventNotificationFn)

	processedTsNs, err := fs.filer.ReadPersistedLogBuffer(lastReadTime, eachLogEntryFn)
	if err != nil {
		return fmt.Errorf("reading from persisted logs: %v", err)
	}

	if processedTsNs != 0 {
		lastReadTime = time.Unix(0, processedTsNs)
	}

	err = fs.metaAggregator.MetaLogBuffer.LoopProcessLogData(lastReadTime, func() bool {
		fs.metaAggregator.ListenersLock.Lock()
		fs.metaAggregator.ListenersCond.Wait()
		fs.metaAggregator.ListenersLock.Unlock()
		return true
	}, eachLogEntryFn)

	return err

}

func (fs *FilerServer) SubscribeLocalMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeLocalMetadataServer) error {

	peerAddress := findClientAddress(stream.Context(), 0)

	clientName := fs.addClient(req.ClientName, peerAddress)

	defer fs.deleteClient(clientName)

	lastReadTime := time.Unix(0, req.SinceNs)
	glog.V(0).Infof(" %v local subscribe %s from %+v", clientName, req.PathPrefix, lastReadTime)

	eachEventNotificationFn := eachEventNotificationFn(req, stream, clientName)

	eachLogEntryFn := eachLogEntryFn(eachEventNotificationFn)

	err := fs.filer.LocalMetaLogBuffer.LoopProcessLogData(lastReadTime, func() bool {
		fs.listenersLock.Lock()
		fs.listenersCond.Wait()
		fs.listenersLock.Unlock()
		return true
	}, eachLogEntryFn)

	return err

}

func eachLogEntryFn(eachEventNotificationFn func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error) func(logEntry *filer_pb.LogEntry) error {
	return func(logEntry *filer_pb.LogEntry) error {
		event := &filer_pb.SubscribeMetadataResponse{}
		if err := proto.Unmarshal(logEntry.Data, event); err != nil {
			glog.Errorf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %v", err)
			return fmt.Errorf("unexpected unmarshal filer_pb.SubscribeMetadataResponse: %v", err)
		}

		if err := eachEventNotificationFn(event.Directory, event.EventNotification, event.TsNs); err != nil {
			return err
		}

		return nil
	}
}

func eachEventNotificationFn(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer, clientName string) func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
	return func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {

		// get complete path to the file or directory
		var entryName string
		if eventNotification.OldEntry != nil {
			entryName = eventNotification.OldEntry.Name
		} else if eventNotification.NewEntry != nil {
			entryName = eventNotification.NewEntry.Name
		}

		fullpath := util.Join(dirPath, entryName)

		// skip on filer internal meta logs
		if strings.HasPrefix(fullpath, filer2.SystemLogDir) {
			return nil
		}

		if !strings.HasPrefix(fullpath, req.PathPrefix) {
			return nil
		}

		message := &filer_pb.SubscribeMetadataResponse{
			Directory:         dirPath,
			EventNotification: eventNotification,
			TsNs:              tsNs,
		}
		if err := stream.Send(message); err != nil {
			glog.V(0).Infof("=> client %v: %+v", clientName, err)
			return err
		}
		return nil
	}
}

func (fs *FilerServer) addClient(clientType string, clientAddress string) (clientName string) {
	clientName = clientType + "@" + clientAddress
	glog.V(0).Infof("+ listener %v", clientName)
	return
}

func (fs *FilerServer) deleteClient(clientName string) {
	glog.V(0).Infof("- listener %v", clientName)
}
