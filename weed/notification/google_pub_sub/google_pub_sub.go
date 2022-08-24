package google_pub_sub

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
)

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &GooglePubSub{})
}

type GooglePubSub struct {
	topic *pubsub.Topic
}

func (k *GooglePubSub) GetName() string {
	return "google_pub_sub"
}

func (k *GooglePubSub) Initialize(configuration util.Configuration, prefix string) (err error) {
	glog.V(0).Infof("notification.google_pub_sub.project_id: %v", configuration.GetString(prefix+"project_id"))
	glog.V(0).Infof("notification.google_pub_sub.topic: %v", configuration.GetString(prefix+"topic"))
	return k.initialize(
		configuration.GetString(prefix+"google_application_credentials"),
		configuration.GetString(prefix+"project_id"),
		configuration.GetString(prefix+"topic"),
	)
}

func (k *GooglePubSub) initialize(google_application_credentials, projectId, topicName string) (err error) {

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

	k.topic = client.Topic(topicName)
	if exists, err := k.topic.Exists(ctx); err == nil {
		if !exists {
			k.topic, err = client.CreateTopic(ctx, topicName)
			if err != nil {
				glog.Fatalf("Failed to create topic %s: %v", topicName, err)
			}
		}
	} else {
		glog.Fatalf("Failed to check topic %s: %v", topicName, err)
	}

	return nil
}

func (k *GooglePubSub) SendMessage(key string, message proto.Message) (err error) {

	bytes, err := proto.Marshal(message)
	if err != nil {
		return
	}

	ctx := context.Background()
	result := k.topic.Publish(ctx, &pubsub.Message{
		Data:       bytes,
		Attributes: map[string]string{"key": key},
	})

	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("send message to google pub sub %s: %v", k.topic.String(), err)
	}

	return nil
}
