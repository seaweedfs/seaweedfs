package sub

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
)

func init() {
	NotificationInputs = append(NotificationInputs, &GooglePubSubInput{})
}

type GooglePubSubInput struct {
	sub         *pubsub.Subscription
	topicName   string
	messageChan chan *pubsub.Message
}

func (k *GooglePubSubInput) GetName() string {
	return "google_pub_sub"
}

func (k *GooglePubSubInput) Initialize(configuration util.Configuration, prefix string) error {
	glog.V(0).Infof("notification.google_pub_sub.project_id: %v", configuration.GetString(prefix+"project_id"))
	glog.V(0).Infof("notification.google_pub_sub.topic: %v", configuration.GetString(prefix+"topic"))
	return k.initialize(
		configuration.GetString(prefix+"google_application_credentials"),
		configuration.GetString(prefix+"project_id"),
		configuration.GetString(prefix+"topic"),
	)
}

func (k *GooglePubSubInput) initialize(google_application_credentials, projectId, topicName string) (err error) {

	ctx := context.Background()
	// Creates a client.
	if google_application_credentials == "" {
		var found bool
		google_application_credentials, found = os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS")
		if !found {
			glog.Fatalf("need to specific GOOGLE_APPLICATION_CREDENTIALS env variable or google_application_credentials in filer.toml")
		}
	}

	client, err := pubsub.NewClient(ctx, projectId, option.WithCredentialsFile(google_application_credentials))
	if err != nil {
		glog.Fatalf("Failed to create client: %v", err)
	}

	k.topicName = topicName
	topic := client.Topic(topicName)
	if exists, err := topic.Exists(ctx); err == nil {
		if !exists {
			topic, err = client.CreateTopic(ctx, topicName)
			if err != nil {
				glog.Fatalf("Failed to create topic %s: %v", topicName, err)
			}
		}
	} else {
		glog.Fatalf("Failed to check topic %s: %v", topicName, err)
	}

	subscriptionName := "seaweedfs_sub"

	k.sub = client.Subscription(subscriptionName)
	if exists, err := k.sub.Exists(ctx); err == nil {
		if !exists {
			k.sub, err = client.CreateSubscription(ctx, subscriptionName, pubsub.SubscriptionConfig{Topic: topic})
			if err != nil {
				glog.Fatalf("Failed to create subscription %s: %v", subscriptionName, err)
			}
		}
	} else {
		glog.Fatalf("Failed to check subscription %s: %v", topicName, err)
	}

	k.messageChan = make(chan *pubsub.Message, 1)

	go k.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		k.messageChan <- m
	})

	return err
}

func (k *GooglePubSubInput) ReceiveMessage() (key string, message *filer_pb.EventNotification, onSuccessFn func(), onFailureFn func(), err error) {

	m := <-k.messageChan

	onSuccessFn = func() {
		m.Ack()
	}
	onFailureFn = func() {
		m.Nack()
	}

	// process the message
	key = m.Attributes["key"]
	message = &filer_pb.EventNotification{}
	err = proto.Unmarshal(m.Data, message)

	if err != nil {
		err = fmt.Errorf("unmarshal message from google pubsub %s: %v", k.topicName, err)
		return
	}

	return
}
