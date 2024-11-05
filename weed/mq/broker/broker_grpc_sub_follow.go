package broker

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
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

	partitionDir := topic.PartitionDir(t, p)
	offsetFileName := fmt.Sprintf("%s.offset", initMessage.ConsumerGroup)

	err = b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		data, err := filer.ReadInsideFiler(client, partitionDir, offsetFileName)
		if err != nil {
			return err
		}
		if len(data) != 8 {
			return fmt.Errorf("no offset found")
		}
		offset = int64(util.BytesToUint64(data))
		return nil
	})
	return offset, err
}

func (b *MessageQueueBroker) saveConsumerGroupOffset(t topic.Topic, p topic.Partition, consumerGroup string, offset int64) error {

	partitionDir := topic.PartitionDir(t, p)
	offsetFileName := fmt.Sprintf("%s.offset", consumerGroup)

	offsetBytes := make([]byte, 8)
	util.Uint64toBytes(offsetBytes, uint64(offset))

	return b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		glog.V(0).Infof("saving topic %s partition %v consumer group %s offset %d", t, p, consumerGroup, offset)
		return filer.SaveInsideFiler(client, partitionDir, offsetFileName, offsetBytes)
	})
}
