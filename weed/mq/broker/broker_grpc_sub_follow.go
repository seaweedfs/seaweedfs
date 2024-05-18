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
	"time"
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
	subscriberProgress := &SubscriberProgress{
		Topic: topic.FromPbTopic(initMessage.Topic),
		Partition: topic.FromPbPartition(initMessage.Partition),
		ConsumerGroup: "consumer_group",
	}

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
			subscriberProgress.Offset = ackMessage.TsNs
			println("offset", subscriberProgress.Offset)
		} else if closeMessage := req.GetClose(); closeMessage != nil {
			glog.V(0).Infof("topic %v partition %v subscribe stream closed: %v", initMessage.Topic, initMessage.Partition, closeMessage)
			return nil
		} else {
			glog.Errorf("unknown message: %v", req)
		}
	}

	t, p := topic.FromPbTopic(initMessage.Topic), topic.FromPbPartition(initMessage.Partition)

	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	partitionGeneration := time.Unix(0, p.UnixTimeNs).UTC().Format(topic.TIME_FORMAT)
	partitionDir := fmt.Sprintf("%s/%s/%04d-%04d", topicDir, partitionGeneration, p.RangeStart, p.RangeStop)
	offsetFileName := fmt.Sprintf("%s.offset", subscriberProgress.ConsumerGroup)

	offsetBytes := make([]byte, 8)
	util.Uint64toBytes(offsetBytes, uint64(subscriberProgress.Offset))

	err = b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		return filer.SaveInsideFiler(client, partitionDir, offsetFileName, offsetBytes)
	})

	glog.V(0).Infof("shut down follower for %v %v", t, p)

	return err
}
