package broker

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

/*
The messages is buffered in memory, and saved to filer under
	/topics/<topic>/<date>/<hour>/<segment>/*.msg
	/topics/<topic>/<date>/<hour>/segment
	/topics/<topic>/info/segment_<id>.meta
*/

func (broker *MessageQueueBroker) Publish(stream mq_pb.SeaweedMessaging_PublishServer) error {
	return nil
}
