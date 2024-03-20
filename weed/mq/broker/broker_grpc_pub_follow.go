package broker

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"io"
)

func (b *MessageQueueBroker) PublishFollowMe(stream mq_pb.SeaweedMessaging_PublishFollowMeServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	initMessage := req.GetInit()
	if initMessage == nil {
		return fmt.Errorf("missing init message")
	}

	// t, p := topic.FromPbTopic(initMessage.Topic), topic.FromPbPartition(initMessage.Partition)
	// follow each published messages
	for {
		// receive a message
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.V(0).Infof("topic %v partition %v publish stream error: %v", initMessage.Topic, initMessage.Partition, err)
			return err
		}

		// Process the received message
		if dataMessage := req.GetData(); dataMessage != nil {
			// send back the ack
			if err := stream.Send(&mq_pb.PublishFollowMeResponse{
				AckTsNs: dataMessage.TsNs,
			}); err != nil {
				// TODO save un-acked messages to disk
				glog.Errorf("Error sending response %v: %v", dataMessage, err)
			}
			println("ack", string(dataMessage.Key), dataMessage.TsNs)
		} else if closeMessage := req.GetClose(); closeMessage != nil {
			glog.V(0).Infof("topic %v partition %v publish stream closed: %v", initMessage.Topic, initMessage.Partition, closeMessage)
			break
		}
	}
	return nil
}
