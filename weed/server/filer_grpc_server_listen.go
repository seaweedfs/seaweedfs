package weed_server

import (
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (fs *FilerServer) ListenForEvents(req *filer_pb.ListenForEventsRequest, stream filer_pb.SeaweedFiler_ListenForEventsServer) error {

	peerAddress := findClientAddress(stream.Context(), 0)

	clientName := fs.addClient(req.ClientName, peerAddress)

	defer fs.deleteClient(clientName)

	lastReadTime := time.Now()
	if req.SinceNs > 0 {
		lastReadTime = time.Unix(0, req.SinceNs)
	}
	var readErr error
	for {

		lastReadTime, readErr = fs.filer.ReadLogBuffer(lastReadTime, func(fullpath string, eventNotification *filer_pb.EventNotification) error {
			if strings.HasPrefix(fullpath, "/.meta") {
				return nil
			}
			if !strings.HasPrefix(fullpath, req.Directory) {
				return nil
			}
			message := &filer_pb.FullEventNotification{
				Directory:         fullpath,
				EventNotification: eventNotification,
			}
			if err := stream.Send(message); err != nil {
				return err
			}
			return nil
		})
		if readErr != nil {
			glog.V(0).Infof("=> client %v: %+v", clientName, readErr)
			return readErr
		}

		fs.listenersLock.Lock()
		fs.listenersCond.Wait()
		fs.listenersLock.Unlock()
	}

	return nil
}

func (fs *FilerServer) addClient(clientType string, clientAddress string) (clientName string) {
	clientName = clientType + "@" + clientAddress
	glog.V(0).Infof("+ listener %v", clientName)
	return
}

func (fs *FilerServer) deleteClient(clientName string) {
	glog.V(0).Infof("- listener %v", clientName)
}

func (fs *FilerServer) notifyMetaListeners() {
	fs.listenersCond.Broadcast()
}
