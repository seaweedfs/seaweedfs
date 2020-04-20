package weed_server

import (
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) SubscribeMetadata(req *filer_pb.SubscribeMetadataRequest, stream filer_pb.SeaweedFiler_SubscribeMetadataServer) error {

	peerAddress := findClientAddress(stream.Context(), 0)

	clientName := fs.addClient(req.ClientName, peerAddress)

	defer fs.deleteClient(clientName)

	lastReadTime := time.Now()
	if req.SinceNs > 0 {
		lastReadTime = time.Unix(0, req.SinceNs)
	}

	var readErr error
	for {

		lastReadTime, readErr = fs.filer.ReadLogBuffer(lastReadTime, func(dirPath string, eventNotification *filer_pb.EventNotification) error {

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
