package weed_server

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (fs *FilerServer) ListenForEvents(req *filer_pb.ListenForEventsRequest, stream filer_pb.SeaweedFiler_ListenForEventsServer) error {

	peerAddress := findClientAddress(stream.Context(), 0)

	clientName, messageChan := fs.addClient(req.ClientName, peerAddress)

	defer fs.deleteClient(clientName, messageChan)

	// ts := time.Unix(req.SinceSec, 0)

	// iterate through old messages
	/*
		for _, message := range ms.Topo.ToVolumeLocations() {
			if err := stream.Send(message); err != nil {
				return err
			}
		}
	*/

	// need to add a buffer here to avoid slow clients
	// also needs to support millions of clients

	for message := range messageChan {
		if err := stream.Send(message); err != nil {
			glog.V(0).Infof("=> client %v: %+v", clientName, message)
			return err
		}
	}

	return nil
}

func (fs *FilerServer) addClient(clientType string, clientAddress string) (clientName string, messageChan chan *filer_pb.FullEventNotification) {
	clientName = clientType + "@" + clientAddress
	glog.V(0).Infof("+ listener %v", clientName)

	messageChan = make(chan *filer_pb.FullEventNotification, 10)

	fs.clientChansLock.Lock()
	fs.clientChans[clientName] = messageChan
	fs.clientChansLock.Unlock()
	return
}

func (fs *FilerServer) deleteClient(clientName string, messageChan chan *filer_pb.FullEventNotification) {
	glog.V(0).Infof("- listener %v", clientName)
	close(messageChan)
	fs.clientChansLock.Lock()
	delete(fs.clientChans, clientName)
	fs.clientChansLock.Unlock()
}

func (fs *FilerServer) sendMessageToClients(dir string, eventNotification *filer_pb.EventNotification) {
	message := &filer_pb.FullEventNotification{
		Directory:         dir,
		EventNotification: eventNotification,
	}
	fs.clientChansLock.RLock()
	for _, ch := range fs.clientChans {
		ch <- message
	}
	fs.clientChansLock.RUnlock()
}
