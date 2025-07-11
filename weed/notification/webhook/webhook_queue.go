package webhook

import (
	"fmt"
	"net/url"

	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

// client defines the interface for transport client
// could be extended to support gRPC
type client interface {
	sendMessage(key string, message proto.Message) error
}

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &WebhookQueue{})
}

type WebhookQueue struct {
	client client
}

type config struct {
	endpoint        string
	authBearerToken string
}

func (c *config) validate() error {
	_, err := url.Parse(c.endpoint)
	if err != nil {
		return fmt.Errorf("invalid webhook endpoint %w", err)
	}

	return nil
}

func (w *WebhookQueue) GetName() string {
	return "webhook"
}

func (w *WebhookQueue) Initialize(configuration util.Configuration, prefix string) error {
	c := &config{
		endpoint:        configuration.GetString(prefix + "endpoint"),
		authBearerToken: configuration.GetString(prefix + "bearer_token"),
	}

	if err := c.validate(); err != nil {
		return err
	}

	return w.initialize(c)
}

func (w *WebhookQueue) initialize(cfg *config) error {
	client, err := newHTTPClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create webhook client: %v", err)
	}
	w.client = client
	return nil
}

func (w *WebhookQueue) SendMessage(key string, message proto.Message) error {
	if w.client == nil {
		return fmt.Errorf("webhook client not initialized")
	}
	return w.client.sendMessage(key, message)
}
