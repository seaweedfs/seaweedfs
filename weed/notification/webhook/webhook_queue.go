package webhook

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &Queue{})
}

type Queue struct {
	router       *message.Router
	queueChannel *gochannel.GoChannel
	config       *config
	client       client
	filter       *filter

	ctx    context.Context
	cancel context.CancelFunc

	// Semaphore for controlling concurrent webhook requests
	sem chan struct{}
}

func (w *Queue) GetName() string {
	return queueName
}

func (w *Queue) SendMessage(key string, msg proto.Message) error {
	eventNotification, ok := msg.(*filer_pb.EventNotification)
	if !ok {
		return nil
	}

	if w.filter != nil && !w.filter.shouldPublish(key, eventNotification) {
		return nil
	}

	m := newWebhookMessage(key, msg)
	if m == nil {
		return nil
	}

	wMsg, err := m.toWaterMillMessage()
	if err != nil {
		return err
	}

	return w.queueChannel.Publish(pubSubTopicName, wMsg)
}

func (w *webhookMessage) toWaterMillMessage() (*message.Message, error) {
	payload, err := proto.Marshal(w.Notification)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)
	// Set event type and key as metadata
	msg.Metadata.Set("event_type", w.EventType)
	msg.Metadata.Set("key", w.Key)

	return msg, nil
}

func (w *Queue) Initialize(configuration util.Configuration, prefix string) error {
	c := newConfigWithDefaults(configuration, prefix)

	if err := c.validate(); err != nil {
		return err
	}

	return w.initialize(c)
}

func (w *Queue) initialize(cfg *config) error {
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.config = cfg
	w.filter = newFilter(cfg)

	// Initialize semaphore for controlling concurrent webhook requests
	w.sem = make(chan struct{}, cfg.nWorkers)

	hClient, err := newHTTPClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create webhook http client: %w", err)
	}
	w.client = hClient

	if err = w.setupWatermillQueue(cfg); err != nil {
		return fmt.Errorf("failed to setup watermill queue: %w", err)
	}
	if err = w.logDeadLetterMessages(); err != nil {
		return err
	}

	return nil
}

func (w *Queue) setupWatermillQueue(cfg *config) error {
	logger := watermill.NewStdLogger(false, false)
	pubSubConfig := gochannel.Config{
		OutputChannelBuffer: int64(cfg.bufferSize),
		Persistent:          false,
	}
	w.queueChannel = gochannel.NewGoChannel(pubSubConfig, logger)

	router, err := message.NewRouter(
		message.RouterConfig{
			CloseTimeout: 60 * time.Second,
		},
		logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create router: %w", err)
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

	poisonQueue, err := middleware.PoisonQueue(w.queueChannel, deadLetterTopic)
	if err != nil {
		return fmt.Errorf("failed to create poison queue: %w", err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(retryMiddleware, poisonQueue)

	// Add a single handler to avoid duplicate message delivery.
	// With gochannel's default behavior, each handler call creates
	// a separate subscription, and all subscriptions receive their own copy of each message.
	// Using a single handler ensures each webhook is sent only once.
	// Concurrency is controlled via semaphore in handleWebhook based on nWorkers config.
	router.AddConsumerHandler(
		"webhook_handler",
		pubSubTopicName,
		w.queueChannel,
		w.handleWebhook,
	)

	go func() {
		// cancels the queue context so the dead letter logger exists in case context not canceled by the shutdown signal already
		defer w.cancel()

		if err := router.Run(w.ctx); err != nil && !errors.Is(err, context.Canceled) {
			glog.Errorf("webhook pubsub worker stopped with error: %v", err)
		}

		glog.Info("webhook pubsub worker stopped")
	}()

	return nil
}

func (w *Queue) handleWebhook(msg *message.Message) error {
	// Acquire semaphore slot (blocks if at capacity)
	w.sem <- struct{}{}
	defer func() { <-w.sem }()

	var n filer_pb.EventNotification
	if err := proto.Unmarshal(msg.Payload, &n); err != nil {
		glog.Errorf("failed to unmarshal protobuf message: %v", err)
		return err
	}

	// Reconstruct webhook message from metadata and payload
	webhookMsg := &webhookMessage{
		Key:          msg.Metadata.Get("key"),
		EventType:    msg.Metadata.Get("event_type"),
		Notification: &n,
	}

	if err := w.client.sendMessage(webhookMsg); err != nil {
		glog.Errorf("failed to send message to webhook %s: %v", webhookMsg.Key, err)
		return err
	}

	return nil
}

func (w *Queue) logDeadLetterMessages() error {
	ch, err := w.queueChannel.Subscribe(w.ctx, deadLetterTopic)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					glog.Info("dead letter channel closed")
					return
				}
				if msg == nil {
					glog.Errorf("received nil message from dead letter channel")
					continue
				}
				key := "unknown"
				if msg.Metadata != nil {
					if keyValue, exists := msg.Metadata["key"]; exists {
						key = keyValue
					}
				}
				payload := ""
				if msg.Payload != nil {
					var n filer_pb.EventNotification
					if err := proto.Unmarshal(msg.Payload, &n); err != nil {
						payload = fmt.Sprintf("failed to unmarshal payload: %v", err)
					} else {
						payload = n.String()
					}
				}
				glog.Errorf("received dead letter message: %s, key: %s", payload, key)
			case <-w.ctx.Done():
				return
			}
		}
	}()

	return nil
}
