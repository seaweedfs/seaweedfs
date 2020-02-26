// Package gocdk_pub_sub supports the Go CDK (Cloud Development Kit) PubSub API,
// which in turn supports many providers, including Amazon SNS/SQS, Azure Service Bus,
// Google Cloud PubSub, and RabbitMQ.
//
// In the config, select a provider and topic using a URL. See
// https://godoc.org/gocloud.dev/pubsub and its sub-packages for details.
//
// The Go CDK PubSub API does not support administrative operations like topic
// creation. Create the topic using a UI, CLI or provider-specific API before running
// weed.
//
// The Go CDK obtains credentials via environment variables and other
// provider-specific default mechanisms. See the provider's documentation for
// details.
package gocdk_pub_sub

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/awssnssqs"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/notification"
	"github.com/chrislusf/seaweedfs/weed/util"
	// _ "gocloud.dev/pubsub/azuresb"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/natspubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"
)

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &GoCDKPubSub{})
}

type GoCDKPubSub struct {
	topicURL string
	topic    *pubsub.Topic
}

func (k *GoCDKPubSub) GetName() string {
	return "gocdk_pub_sub"
}

func (k *GoCDKPubSub) Initialize(configuration util.Configuration, prefix string) error {
	k.topicURL = configuration.GetString(prefix + "topic_url")
	glog.V(0).Infof("notification.gocdk_pub_sub.topic_url: %v", k.topicURL)
	topic, err := pubsub.OpenTopic(context.Background(), k.topicURL)
	if err != nil {
		glog.Fatalf("Failed to open topic: %v", err)
	}
	k.topic = topic
	return nil
}

func (k *GoCDKPubSub) SendMessage(key string, message proto.Message) error {
	bytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	err = k.topic.Send(context.Background(), &pubsub.Message{
		Body:     bytes,
		Metadata: map[string]string{"key": key},
	})
	if err != nil {
		return fmt.Errorf("send message via Go CDK pubsub %s: %v", k.topicURL, err)
	}
	return nil
}
