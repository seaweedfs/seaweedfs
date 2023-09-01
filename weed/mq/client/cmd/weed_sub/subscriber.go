package main

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
)

func main() {

	subscriber := sub_client.NewTopicSubscriber(
		&sub_client.SubscriberConfiguration{
			ConsumerGroup: "test",
			ConsumerId:    "test",
		},
		"test", "test")
	if err := subscriber.Connect("localhost:17777"); err != nil {
		fmt.Println(err)
		return
	}

	if err := subscriber.Subscribe(func(key, value []byte) bool {
		println(string(key), "=>", string(value))
		return true
	}, func() {
		println("done subscribing")
	}); err != nil {
		fmt.Println(err)
	}

}
