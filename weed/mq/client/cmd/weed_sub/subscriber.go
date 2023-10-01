package main

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	subscriberConfig := &sub_client.SubscriberConfiguration{
		ClientId:        "testSubscriber",
		GroupId:         "test",
		GroupInstanceId: "test",
		GrpcDialOption:  grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	contentConfig := &sub_client.ContentConfiguration{
		Namespace: "test",
		Topic:     "test",
		Filter:    "",
	}

	subscriber := sub_client.NewTopicSubscriber("localhost:17777", subscriberConfig, contentConfig)

	subscriber.SetEachMessageFunc(func(key, value []byte) bool {
		println(string(key), "=>", string(value))
		return true
	})

	subscriber.SetCompletionFunc(func() {
		println("done subscribing")
	})

	if err := subscriber.Subscribe(); err != nil {
		fmt.Println(err)
	}

}
