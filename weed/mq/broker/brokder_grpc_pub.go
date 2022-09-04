package broker

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/message_fbs"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

/*
The messages is buffered in memory, and saved to filer under
	/topics/<topic>/<date>/<hour>/<segment>/*.msg
	/topics/<topic>/<date>/<hour>/segment
	/topics/<topic>/info/segment_<id>.meta
*/

func (broker *MessageQueueBroker) PublishMessage(stream mq_pb.SeaweedMessaging_PublishMessageServer) error {
	println("connected")
	for {
		request, recvErr := stream.Recv()
		if recvErr != nil {
			return recvErr
		}

		print(">")
		if request.Control != nil {

		}
		if request.Data != nil {
			if err := broker.processDataMessage(stream, request.Data); err != nil {
				return err
			}
		}

	}
	return nil
}

func (broker *MessageQueueBroker) processDataMessage(stream mq_pb.SeaweedMessaging_PublishMessageServer, data *mq_pb.PublishRequest_DataMessage) error {
	mb := message_fbs.GetRootAsMessageBatch(data.Message, 0)

	println("message count:", mb.MessagesLength(), len(data.Message))
	m := &message_fbs.Message{}
	for i := 0; i < mb.MessagesLength(); i++ {
		mb.Messages(m, i)
		println(i, ">", string(m.Data()))
	}
	return nil
}
