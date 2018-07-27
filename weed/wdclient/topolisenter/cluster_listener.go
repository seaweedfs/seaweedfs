package clusterlistener

import (
	"context"
	"sync"
	"time"

	"code.uber.internal/fraud/alpine/.gen/proto/go/fraud/alpine"
	"code.uber.internal/fraud/alpine/server/util"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

type Location struct {
	Url       string
	PublicUrl string
}

type ClusterListener struct {
	sync.RWMutex
	vid2locations map[storage.VolumeId][]*Location
	clientName    string
}

func NewClusterListener(clientName string) *ClusterListener {
	return &ClusterListener{
		vid2locations: make(map[storage.VolumeId][]*Location),
		clientName:    clientName,
	}
}

// StartListener keeps the listener connected to the master.
func (clusterListener *ClusterListener) StartListener(ctx context.Context, master string) {

	clusterUpdatesChan := make(chan *pb.ClusterStatusMessage)

	go util.RetryForever(ctx, clusterListener.clientName+" cluster listener", func() error {
		return clusterListener.establishConnectionWithMaster(master, clusterUpdatesChan)
	}, 2*time.Second)

	go func() {
		for {
			select {
			case msg := <-clusterUpdatesChan:
				clusterListener.processClusterStatusMessage(msg)
			}
		}
	}()

	// println("client is connected to master", master, "data center", dataCenter)

	return

}

func (clusterListener *ClusterListener) processClusterStatusMessage(msg *pb.ClusterStatusMessage) {
}
