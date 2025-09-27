package broker

import (
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func (b *MessageQueueBroker) SubscribeFollowMe(stream mq_pb.SeaweedMessaging_SubscribeFollowMeServer) (err error) {
	var req *mq_pb.SubscribeFollowMeRequest
	req, err = stream.Recv()
	if err != nil {
		return err
	}
	initMessage := req.GetInit()
	if initMessage == nil {
		return fmt.Errorf("missing init message")
	}

	// create an in-memory offset
	var lastOffset int64

	// follow each published messages
	for {
		// receive a message
		req, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			glog.V(0).Infof("topic %v partition %v subscribe stream error: %v", initMessage.Topic, initMessage.Partition, err)
			break
		}

		// Process the received message
		if ackMessage := req.GetAck(); ackMessage != nil {
			lastOffset = ackMessage.TsNs
			// println("sub follower got offset", lastOffset)
		} else if closeMessage := req.GetClose(); closeMessage != nil {
			glog.V(0).Infof("topic %v partition %v subscribe stream closed: %v", initMessage.Topic, initMessage.Partition, closeMessage)
			return nil
		} else {
			glog.Errorf("unknown message: %v", req)
		}
	}

	t, p := topic.FromPbTopic(initMessage.Topic), topic.FromPbPartition(initMessage.Partition)

	if lastOffset > 0 {
		err = b.saveConsumerGroupOffset(t, p, initMessage.ConsumerGroup, lastOffset)
	}

	glog.V(0).Infof("shut down follower for %v offset %d", initMessage, lastOffset)

	return err
}

func (b *MessageQueueBroker) readConsumerGroupOffset(initMessage *mq_pb.SubscribeMessageRequest_InitMessage) (offset int64, err error) {
	t, p := topic.FromPbTopic(initMessage.Topic), topic.FromPbPartition(initMessage.PartitionOffset.Partition)

	// Use the offset manager's consumer group storage
	return b.offsetManager.LoadConsumerGroupOffset(t, p, initMessage.ConsumerGroup)
}

func (b *MessageQueueBroker) saveConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string, offset int64) error {
	// Use the offset manager's consumer group storage
	glog.V(0).Infof("saving topic %s partition %v consumer group %s offset %d", t, p, consumerGroup, offset)
	return b.offsetManager.SaveConsumerGroupOffset(t, p, consumerGroup, offset)
}
