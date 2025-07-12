package webhook

import (
	"context"
	"encoding/json"
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
}

func (w *Queue) GetName() string {
	return QueueName
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
	wMsg, err := m.toWaterMillMessage()
	if err != nil {
		return err
	}

	return w.queueChannel.Publish(pubSubTopicName, wMsg)
}

func (w *webhookMessage) toWaterMillMessage() (*message.Message, error) {
	payload, err := json.Marshal(w)
	if err != nil {
		return nil, err
	}

	return message.NewMessage(watermill.NewUUID(), payload), nil
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

	poisonQueue, err := middleware.PoisonQueue(w.queueChannel, deadLetterTopic)
	if err != nil {
		return fmt.Errorf("failed to create poison queue: %v", err)
	}

	router.AddPlugin(plugin.SignalsHandler)
	router.AddMiddleware(retryMiddleware, poisonQueue)

	for i := 0; i < cfg.nWorkers; i++ {
		router.AddNoPublisherHandler(
			pubSubHandlerNameTemplate(i),
			pubSubTopicName,
			w.queueChannel,
			w.handleWebhook,
		)
	}

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

func (w *Queue) logDeadLetterMessages() error {
	ch, err := w.queueChannel.Subscribe(w.ctx, deadLetterTopic)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case msg := <-ch:
				glog.Errorf("received dead letter message: %v", msg)
			case <-w.ctx.Done():
				return
			}
		}
	}()

	return nil
}
