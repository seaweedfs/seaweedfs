package sub_client

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
)

// Subscribe subscribes to a topic's specified partitions.
// If a partition is moved to another broker, the subscriber will automatically reconnect to the new broker.

func (sub *TopicSubscriber) Subscribe() error {
	util.RetryUntil("subscribe", func() error {
		// ask balancer for brokers of the topic
		if err := sub.doLookup(sub.bootstrapBroker); err != nil {
			return fmt.Errorf("lookup topic %s/%s: %v", sub.ContentConfig.Namespace, sub.ContentConfig.Topic, err)
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
			return false
		}
		return true
	})
	return nil
}
