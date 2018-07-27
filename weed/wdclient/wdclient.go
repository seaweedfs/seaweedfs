package wdclient

import (
	"context"
	"code.uber.internal/fraud/alpine/.gen/proto/go/fraud/alpine"
)

type SeaweedClient struct {
	ctx             context.Context
	Master          string
	ClientName      string
	ClusterListener *clusterlistener.ClusterListener
}

// NewSeaweedClient creates a SeaweedFS client which contains a listener for the Seaweed system topology changes
func NewSeaweedClient(ctx context.Context, clientName, master string) *SeaweedClient {
	c := &SeaweedClient{
		ctx:             ctx,
		Master:          master,
		ClusterListener: clusterlistener.NewClusterListener(clientName),
		ClientName:      clientName,
	}
	c.ClusterListener.StartListener(ctx, c.Master)

	conn, err := grpc.Dial(c.Master, grpc.WithInsecure())
	if err != nil {
		glog.Fatalf("%s fail to dial %v: %v", c.ClientName, c.Master, err)
	}
	c.MasterClient = pb.NewAlpineMasterClient(conn)

	return c
}

// NewClusterClient create a lightweight client to access a specific cluster
// TODO The call will block if the keyspace is not created in this data center.
func (c *SeaweedClient) NewClusterClient(keyspace string) (clusterClient *ClusterClient) {

	return &ClusterClient{
		keyspace: keyspace,
	}

}
