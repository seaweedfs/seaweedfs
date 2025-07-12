package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

const (
	QueueName       = "webhook"
	pubSubTopicName = "webhook_topic"
	DeadLetterTopic = "webhook_dead_letter"
)

var (
	pubSubHandlerNameTemplate = func(n int) string {
		return "webhook_handler_" + strconv.Itoa(n)
	}
)

type client interface {
	sendMessage(message *webhookMessage) error
}

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &Queue{})
}

type webhookMessage struct {
	Key         string          `json:"key"`
	MessageData json.RawMessage `json:"message_data"`
	Message     proto.Message   `json:"-"`
}
type Queue struct {
	router    *message.Router
	publisher message.Publisher
	config    *config
	client    client
}

func (w *Queue) GetName() string {
	return QueueName
}

func (w *Queue) SendMessage(key string, msg proto.Message) error {
	wMsg, err := newWebhookMessage(key, msg).toWaterMillMessage()
	if err != nil {
		return err
	}

	return w.publisher.Publish(pubSubTopicName, wMsg)
}

func newWebhookMessage(key string, message proto.Message) *webhookMessage {
	messageData, _ := json.Marshal(message)

	return &webhookMessage{
		Key:         key,
		MessageData: messageData,
		Message:     message,
	}
}

func (w *webhookMessage) toWaterMillMessage() (*message.Message, error) {
	payload, err := json.Marshal(w)
	if err != nil {
		return nil, err
	}

	return message.NewMessage(watermill.NewUUID(), payload), nil
}

type config struct {
	endpoint        string
	authBearerToken string
	timeoutSeconds  int

	maxRetries        int
	backoffSeconds    int
	maxBackoffSeconds int
	nWorkers          int
	bufferSize        int
}

func newConfigWithDefaults(configuration util.Configuration, prefix string) *config {
	c := &config{
		endpoint:          configuration.GetString(prefix + "endpoint"),
		authBearerToken:   configuration.GetString(prefix + "bearer_token"),
		timeoutSeconds:    10,
		maxRetries:        3,
		backoffSeconds:    3,
		maxBackoffSeconds: 30,
		nWorkers:          5,
		bufferSize:        10_000,
	}

	if bufferSize := configuration.GetInt(prefix + "buffer_size"); bufferSize > 0 {
		c.bufferSize = bufferSize
	}
	if workers := configuration.GetInt(prefix + "workers"); workers > 0 {
		c.nWorkers = workers
	}
	if maxRetries := configuration.GetInt(prefix + "max_retries"); maxRetries > 0 {
		c.maxRetries = maxRetries
	}
	if backoffSeconds := configuration.GetInt(prefix + "backoff_seconds"); backoffSeconds > 0 {
		c.backoffSeconds = backoffSeconds
	}
	if timeout := configuration.GetInt(prefix + "timeout_seconds"); timeout > 0 {
		c.timeoutSeconds = timeout
	}

	return c
}

func (c *config) validate() error {
	if c.endpoint == "" {
		return fmt.Errorf("webhook endpoint is required")
	}

	_, err := url.Parse(c.endpoint)
	if err != nil {
		return fmt.Errorf("invalid webhook endpoint: %w", err)
	}

	if c.timeoutSeconds < 1 || c.timeoutSeconds > 300 {
		return fmt.Errorf("timeout must be between 1 and 300 seconds, got %d", c.timeoutSeconds)
	}

	if c.maxRetries < 0 || c.maxRetries > 10 {
		return fmt.Errorf("max retries must be between 0 and 10, got %d", c.maxRetries)
	}

	if c.backoffSeconds < 1 || c.backoffSeconds > 60 {
		return fmt.Errorf("backoff seconds must be between 1 and 60, got %d", c.backoffSeconds)
	}

	if c.maxBackoffSeconds < c.backoffSeconds || c.maxBackoffSeconds > 300 {
		return fmt.Errorf("max backoff seconds must be between %d and 300, got %d", c.backoffSeconds, c.maxBackoffSeconds)
	}

	if c.nWorkers < 1 || c.nWorkers > 100 {
		return fmt.Errorf("workers must be between 1 and 100, got %d", c.nWorkers)
	}

	if c.bufferSize < 100 || c.bufferSize > 1_000_000 {
		return fmt.Errorf("buffer size must be between 100 and 1,000,000, got %d", c.bufferSize)
	}

	return nil
}

func (w *Queue) Initialize(configuration util.Configuration, prefix string) error {
	c := newConfigWithDefaults(configuration, prefix)

	if err := c.validate(); err != nil {
		return err
	}

	return w.initialize(c)
}

func (w *Queue) initialize(cfg *config) error {
	w.config = cfg

	client, err := newHTTPClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create webhook client: %w", err)
	}
	w.client = client

	err = w.setupWatermillQueue(cfg)
	if err != nil {
		return fmt.Errorf("failed to setup watermill queue: %w", err)
	}

	return nil
}

func (w *Queue) setupWatermillQueue(cfg *config) error {
	logger := watermill.NewStdLogger(false, false)

	pubSubConfig := gochannel.Config{
		OutputChannelBuffer: int64(cfg.bufferSize),
		Persistent:          false,
	}
	pubSubChannel := gochannel.NewGoChannel(pubSubConfig, logger)
	w.publisher = pubSubChannel

	router, err := message.NewRouter(
		message.RouterConfig{
			CloseTimeout: 60 * time.Second,
		},
		logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create router: %v", err)
	}
	w.router = router

	retryMiddleware := middleware.Retry{
		MaxRetries:          cfg.maxRetries,
		InitialInterval:     time.Duration(cfg.backoffSeconds) * time.Second,
		MaxInterval:         time.Duration(cfg.maxBackoffSeconds) * time.Second,
		Multiplier:          2.0,
		RandomizationFactor: 0.3,
		Logger:              logger,
	}.Middleware

	poisonQueue, err := middleware.PoisonQueue(pubSubChannel, DeadLetterTopic)
	if err != nil {
		return fmt.Errorf("failed to create poison queue: %v", err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(retryMiddleware, poisonQueue)

	for i := 0; i < cfg.nWorkers; i++ {
		router.AddNoPublisherHandler(
			pubSubHandlerNameTemplate(i),
			pubSubTopicName,
			pubSubChannel,
			w.handleWebhook,
		)
	}

	go func() {
		if err := router.Run(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
			glog.Errorf("webhook pubsub worker stopped with error: %v", err)
		}

		glog.Info("webhook pubsub worker stopped")
	}()

	return nil
}

func (w *Queue) handleWebhook(msg *message.Message) error {
	var webhookMsg webhookMessage
	if err := json.Unmarshal(msg.Payload, &webhookMsg); err != nil {
		glog.Errorf("failed to unmarshal message: %v", err)
		return err
	}

	if err := w.client.sendMessage(&webhookMsg); err != nil {
		glog.Errorf("failed to send message to webhook %s: %v", webhookMsg.Key, err)
		return err
	}

	return nil
}

func (w *Queue) GetDeadLetterMessages() (<-chan *message.Message, error) {
	if w.publisher == nil {
		return nil, fmt.Errorf("queue not initialized")
	}

	subscriber, ok := w.publisher.(message.Subscriber)
	if !ok {
		return nil, fmt.Errorf("publisher does not support subscribing")
	}

	return subscriber.Subscribe(context.Background(), DeadLetterTopic)
}
