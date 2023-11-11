package sub_client

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"log"
	"time"
)

// Subscribe subscribes to a topic's specified partitions.
// If a partition is moved to another broker, the subscriber will automatically reconnect to the new broker.

func (sub *TopicSubscriber) Subscribe() error {
	index := -1
	util.RetryUntil("subscribe", func() error {
		index++
		index = index % len(sub.bootstrapBrokers)
		// ask balancer for brokers of the topic
		if err := sub.doLookup(sub.bootstrapBrokers[index]); err != nil {
			return fmt.Errorf("lookup topic %s/%s: %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, err)
		}
		if len(sub.brokerPartitionAssignments) == 0 {
			if sub.waitForMoreMessage {
				time.Sleep(1 * time.Second)
				return fmt.Errorf("no broker partition assignments")
			} else {
				return nil
			}
		}
		// treat the first broker as the topic leader
		// connect to the leader broker

		// subscribe to the topic
		if err := sub.doProcess(); err != nil {
			return fmt.Errorf("subscribe topic %s/%s: %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, err)
		}
		return nil
	}, func(err error) bool {
		if err == io.EOF {
			log.Printf("subscriber %s/%s: %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, err)
			sub.waitForMoreMessage = false
			return false
		}
		return true
	})
	return nil
}
