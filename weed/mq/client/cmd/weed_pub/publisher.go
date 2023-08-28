package main

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/pub_client"
)

func main() {

	publisher := pub_client.NewTopicPublisher(
		"test", "test")
	if err := publisher.Connect("localhost:17777"); err != nil {
		fmt.Println(err)
		return
	}

	for i := 0; i < 10; i++ {
		if dataErr := publisher.Publish(
			[]byte(fmt.Sprintf("key-%d", i)),
			[]byte(fmt.Sprintf("value-%d", i)),
		); dataErr != nil {
			fmt.Println(dataErr)
			return
		}
	}

	fmt.Println("done publishing")

}
