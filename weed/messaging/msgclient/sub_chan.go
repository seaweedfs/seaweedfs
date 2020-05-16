package msgclient

import (
	"crypto/md5"
	"hash"
	"io"
	"log"
	"time"

	"github.com/chrislusf/seaweedfs/weed/messaging/broker"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
)

type SubChannel struct {
	ch      chan []byte
	stream  messaging_pb.SeaweedMessaging_SubscribeClient
	md5hash hash.Hash
}

func (mc *MessagingClient) NewSubChannel(subscriberId, chanName string) (*SubChannel, error) {
	tp := broker.TopicPartition{
		Namespace: "chan",
		Topic:     chanName,
		Partition: 0,
	}
	grpcConnection, err := mc.findBroker(tp)
	if err != nil {
		return nil, err
	}
	sc, err := setupSubscriberClient(grpcConnection, tp, subscriberId, time.Unix(0, 0))
	if err != nil {
		return nil, err
	}

	t := &SubChannel{
		ch:      make(chan []byte),
		stream:  sc,
		md5hash: md5.New(),
	}

	go func() {
		for {
			resp, subErr := t.stream.Recv()
			if subErr == io.EOF {
				return
			}
			if subErr != nil {
				log.Printf("fail to receive from netchan %s: %v", chanName, subErr)
				return
			}
			if resp.Data == nil {
				// this could be heartbeat from broker
				continue
			}
			if resp.Data.IsClose {
				t.stream.Send(&messaging_pb.SubscriberMessage{
					IsClose: true,
				})
				close(t.ch)
				return
			}
			t.ch <- resp.Data.Value
			t.md5hash.Write(resp.Data.Value)
		}
	}()

	return t, nil
}

func (sc *SubChannel) Channel() chan []byte {
	return sc.ch
}

func (sc *SubChannel) Md5() []byte {
	return sc.md5hash.Sum(nil)
}
