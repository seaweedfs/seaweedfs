package webhook

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// client defines the interface for transport client
// could be extended to support gRPC
type client interface {
	sendMessage(message *webhookMessage) error
}

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &WebhookQueue{})
}

type WebhookQueue struct {
	client     client
	config     *config
	bufferChan chan *webhookMessage
	worker     errgroup.Group
	ctx        context.Context
}

type webhookMessage struct {
	Key     string        `json:"key"`
	Message proto.Message `json:"message"`
}
type config struct {
	endpoint        string
	authBearerToken string

	maxRetries     int
	backoffSeconds int
	nWorkers       int
	bufferSize     int
}

func newConfigWithDefaults(configuration util.Configuration, prefix string) *config {
	c := &config{
		endpoint:        configuration.GetString(prefix + "endpoint"),
		authBearerToken: configuration.GetString(prefix + "bearer_token"),
		maxRetries:      configuration.GetInt(prefix + "max_retries"),
		backoffSeconds:  configuration.GetInt(prefix + "backoff_seconds"),
		nWorkers:        configuration.GetInt(prefix + "workers"),
		bufferSize:      configuration.GetInt(prefix + "buffer_size"),
	}
	if c.nWorkers <= 0 {
		c.nWorkers = 20
	}
	if c.bufferSize <= 0 {
		c.bufferSize = 1000
	}
	if c.backoffSeconds <= 0 {
		c.backoffSeconds = 5
	}

	return c
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
	c := newConfigWithDefaults(configuration, prefix)

	if err := c.validate(); err != nil {
		return err
	}

	return w.initialize(c)
}

func (w *WebhookQueue) initialize(cfg *config) error {
	w.config = cfg

	client, err := newHTTPClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create webhook client: %v", err)
	}
	w.client = client

	go w.setupGracefulShutdown()
	go w.startWorker()

	return nil
}

func (w *WebhookQueue) setupGracefulShutdown() {
	ctx, cancel := context.WithCancel(context.Background())
	w.ctx = ctx

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	cancel()
}

func (w *WebhookQueue) SendMessage(key string, message proto.Message) error {
	if w.client == nil {
		return fmt.Errorf("webhook client not initialized")
	}

	w.bufferChan <- &webhookMessage{
		Key:     key,
		Message: message,
	}

	return nil
}

func (w *WebhookQueue) startWorker() {
	for i := 0; i < w.config.nWorkers; i++ {
		w.worker.Go(func() error {
			for {
				select {
				case <-w.ctx.Done():
					return w.ctx.Err()
				case message := <-w.bufferChan:
					if err := w.client.sendMessage(message); err != nil {
						glog.Errorf("failed to send message to webhook %s: %v", message.Key, err)
					}
				}
			}
		})
	}

	if err := w.worker.Wait(); err != nil {
		glog.Errorf("webhook worker exited with error: %v", err)
	}
}
