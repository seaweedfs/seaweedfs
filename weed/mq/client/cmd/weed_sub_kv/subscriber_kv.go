package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"strings"
	"time"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	namespace               = flag.String("ns", "test", "namespace")
	t                       = flag.String("topic", "test", "topic")
	seedBrokers             = flag.String("brokers", "localhost:17777", "seed brokers")
	maxPartitionCount       = flag.Int("maxPartitionCount", 3, "max partition count")
	perPartitionConcurrency = flag.Int("perPartitionConcurrency", 1, "per partition concurrency")

	clientId = flag.Uint("client_id", uint(util.RandomInt32()), "client id")
)

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	subscriberConfig := &sub_client.SubscriberConfiguration{
		ConsumerGroup:           "test",
		ConsumerGroupInstanceId: fmt.Sprintf("client-%d", *clientId),
		GrpcDialOption:          grpc.WithTransportCredentials(insecure.NewCredentials()),
		MaxPartitionCount:       int32(*maxPartitionCount),
		PerPartitionConcurrency: int32(*perPartitionConcurrency),
	}

	contentConfig := &sub_client.ContentConfiguration{
		Topic:     topic.NewTopic(*namespace, *t),
		Filter:    "",
		StartTime: time.Unix(1, 1),
	}

	brokers := strings.Split(*seedBrokers, ",")
	subscriber := sub_client.NewTopicSubscriber(brokers, subscriberConfig, contentConfig)

	counter := 0
	subscriber.SetEachMessageFunc(func(key, value []byte) error {
		counter++
		println(string(key), "=>", string(value), counter)
		return nil
	})

	subscriber.SetCompletionFunc(func() {
		glog.V(0).Infof("done received %d messages", counter)
	})

	if err := subscriber.Subscribe(); err != nil {
		fmt.Println(err)
	}

}
