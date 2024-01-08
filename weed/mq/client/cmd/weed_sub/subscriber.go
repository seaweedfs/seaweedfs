package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"strings"
	"time"
)

var (
	namespace   = flag.String("ns", "test", "namespace")
	topic       = flag.String("topic", "test", "topic")
	seedBrokers = flag.String("brokers", "localhost:17777", "seed brokers")
)

func main() {
	flag.Parse()

	subscriberConfig := &sub_client.SubscriberConfiguration{
		ClientId:                "testSubscriber",
		ConsumerGroup:           "test",
		ConsumerGroupInstanceId: "test",
		GrpcDialOption:          grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	contentConfig := &sub_client.ContentConfiguration{
		Namespace: *namespace,
		Topic:     *topic,
		Filter:    "",
		StartTime: time.Unix(1, 1),
	}

	processorConfig := sub_client.ProcessorConfiguration{
		ConcurrentPartitionLimit: 6,
	}

	brokers := strings.Split(*seedBrokers, ",")
	subscriber := sub_client.NewTopicSubscriber(brokers, subscriberConfig, contentConfig, processorConfig)

	counter := 0
	subscriber.SetEachMessageFunc(func(key, value []byte) (bool, error) {
		counter++
		println(string(key), "=>", string(value), counter)
		return true, nil
	})

	subscriber.SetCompletionFunc(func() {
		glog.V(0).Infof("done recived %d messages", counter)
	})

	if err := subscriber.Subscribe(); err != nil {
		fmt.Println(err)
	}

}
