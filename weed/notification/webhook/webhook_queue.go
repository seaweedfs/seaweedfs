package webhook

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
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

// target is one webhook endpoint with its own filter, http client and
// concurrency budget. The flat [notification.webhook] section is the unnamed
// target; each enabled [notification.webhook.<name>] subsection adds another.
type target struct {
	name    string
	config  *config
	filter  *filter
	client  client
	sem     chan struct{}
	inbox   chan *message.Message
	pending chan struct{}
}

func newTarget(name string, cfg *config) (*target, error) {
	t := &target{
		name:   name,
		config: cfg,
		filter: newFilter(cfg),
		sem:     make(chan struct{}, cfg.nWorkers),
		inbox:   make(chan *message.Message, cfg.bufferSize),
		pending: make(chan struct{}, cfg.bufferSize),
	}
	hClient, err := newHTTPClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create webhook http client: %w", err)
	}
	t.client = hClient
	return t, nil
}

func (t *target) topic() string {
	if t.name == "" {
		return pubSubTopicName
	}
	return pubSubTopicName + "_" + t.name
}

func (t *target) handlerName() string {
	if t.name == "" {
		return "webhook_handler"
	}
	return "webhook_handler_" + t.name
}

type Queue struct {
	routers      []*message.Router
	queueChannel *gochannel.GoChannel
	targets      []*target

	ctx    context.Context
	cancel context.CancelFunc
}

func (w *Queue) GetName() string {
	return queueName
}

// HasEnabledSubconfig reports whether any [notification.webhook.<name>]
// subsection is enabled, so the queue activates on named targets even when the
// flat [notification.webhook] section is absent.
func (w *Queue) HasEnabledSubconfig(configuration util.Configuration, prefix string) bool {
	return len(namedTargetPrefixes(configuration, prefix)) > 0
}

// stringMapConfig is implemented by *util.ViperProxy; anything narrower simply
// gets no named-target discovery.
type stringMapConfig interface {
	GetStringMap(key string) map[string]interface{}
}

func namedTargetPrefixes(configuration util.Configuration, prefix string) []string {
	mg, ok := configuration.(stringMapConfig)
	if !ok {
		return nil
	}
	var prefixes []string
	for k, v := range mg.GetStringMap(strings.TrimSuffix(prefix, ".")) {
		if _, ok := v.(map[string]interface{}); !ok {
			continue
		}
		p := prefix + k + "."
		if !configuration.GetBool(p + "enabled") {
			continue
		}
		prefixes = append(prefixes, p)
	}
	sort.Strings(prefixes)
	return prefixes
}

func buildTargets(configuration util.Configuration, prefix string) ([]*target, error) {
	var prefixes []string
	if configuration.GetBool(prefix + "enabled") {
		prefixes = append(prefixes, prefix)
	}
	prefixes = append(prefixes, namedTargetPrefixes(configuration, prefix)...)

	var targets []*target
	for _, p := range prefixes {
		cfg := newConfigWithDefaults(configuration, p)
		if err := cfg.validate(); err != nil {
			return nil, err
		}
		t, err := newTarget(strings.TrimSuffix(strings.TrimPrefix(p, prefix), "."), cfg)
		if err != nil {
			return nil, err
		}
		targets = append(targets, t)
	}
	return targets, nil
}

func (w *Queue) SendMessage(key string, msg proto.Message) error {
	eventNotification, ok := msg.(*filer_pb.EventNotification)
	if !ok {
		return nil
	}

	for _, t := range w.targets {
		if t.filter != nil && !t.filter.shouldPublish(key, eventNotification) {
			continue
		}
		m := newWebhookMessage(key, msg)
		if m == nil {
			continue
		}
		wMsg, err := m.toWaterMillMessage()
		if err != nil {
			return err
		}
		wMsg.Metadata.Set("target", t.name)
		// a saturated target applies backpressure instead of dropping the
		// event; the token covers the delivery until the handler finishes
		select {
		case t.pending <- struct{}{}:
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
		select {
		case t.inbox <- wMsg:
		case <-w.ctx.Done():
			t.releasePending()
			return w.ctx.Err()
		}
	}
	return nil
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
	targets, err := buildTargets(configuration, prefix)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return fmt.Errorf("webhook endpoint is required")
	}

	return w.initialize(targets)
}

func (w *Queue) initialize(targets []*target) error {
	w.ctx, w.cancel = context.WithCancel(context.Background())
	w.targets = targets

	if err := w.setupWatermillQueue(targets); err != nil {
		return fmt.Errorf("failed to setup watermill queue: %w", err)
	}
	if err := w.logDeadLetterMessages(); err != nil {
		return err
	}

	return nil
}

func (w *Queue) setupWatermillQueue(targets []*target) error {
	logger := watermill.NewStdLogger(false, false)
	bufferSize := 0
	for _, t := range targets {
		if t.config.bufferSize > bufferSize {
			bufferSize = t.config.bufferSize
		}
	}
	pubSubConfig := gochannel.Config{
		OutputChannelBuffer: int64(bufferSize),
		Persistent:          false,
	}
	w.queueChannel = gochannel.NewGoChannel(pubSubConfig, logger)

	var wg sync.WaitGroup
	for _, t := range targets {
		// One router per target so each keeps its own retry/backoff settings.
		// Handlers subscribe to the target's own topic, so a message is
		// delivered to an endpoint exactly once per matching target.
		router, err := message.NewRouter(
			message.RouterConfig{
				CloseTimeout: 60 * time.Second,
			},
			logger,
		)
		if err != nil {
			return fmt.Errorf("failed to create router: %w", err)
		}
		w.routers = append(w.routers, router)

		retryMiddleware := middleware.Retry{
			MaxRetries:          t.config.maxRetries,
			InitialInterval:     time.Duration(t.config.backoffSeconds) * time.Second,
			MaxInterval:         time.Duration(t.config.maxBackoffSeconds) * time.Second,
			Multiplier:          2.0,
			RandomizationFactor: 0.3,
			Logger:              logger,
		}.Middleware

		poisonQueue, err := middleware.PoisonQueue(w.queueChannel, deadLetterTopic)
		if err != nil {
			return fmt.Errorf("failed to create poison queue: %w", err)
		}

		router.AddPlugin(plugin.SignalsHandler)
		// outermost: the reservation SendMessage took releases once the whole
		// chain (retries, dead letter) is done, bounding in-flight deliveries
		releasePending := func(h message.HandlerFunc) message.HandlerFunc {
			return func(msg *message.Message) ([]*message.Message, error) {
				defer t.releasePending()
				return h(msg)
			}
		}
		router.AddMiddleware(releasePending, retryMiddleware, poisonQueue)

		t := t
		router.AddConsumerHandler(
			t.handlerName(),
			t.topic(),
			w.queueChannel,
			t.handleWebhook,
		)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := router.Run(w.ctx); err != nil && !errors.Is(err, context.Canceled) {
				glog.Errorf("webhook pubsub worker stopped with error: %v", err)
			}
			glog.Info("webhook pubsub worker stopped")
		}()

		go w.forwardTargetMessages(t)

		// without a subscriber GoChannel drops the publish as undeliverable
		select {
		case <-router.Running():
		case <-w.ctx.Done():
			return w.ctx.Err()
		}
	}

	// the queue context lives until every target's worker has stopped so the
	// dead letter logger keeps running while any of them is still up
	go func() {
		wg.Wait()
		w.cancel()
	}()

	return nil
}

// forwardTargetMessages publishes each queued event to the target's topic in
// order. Publish detaches its delivery, so the pending reservation is what
// keeps a saturated target from queuing without bound.
func (w *Queue) forwardTargetMessages(t *target) {
	for {
		select {
		case msg := <-t.inbox:
			if err := w.queueChannel.Publish(t.topic(), msg); err != nil {
				glog.Errorf("webhook target %q failed to publish message: %v", t.name, err)
				t.releasePending()
			}
		case <-w.ctx.Done():
			return
		}
	}
}

// releasePending frees one delivery reservation. It never blocks: a nacked
// message GoChannel resends reaches the release again after its token is gone.
func (t *target) releasePending() {
	select {
	case <-t.pending:
	default:
	}
}

func (t *target) handleWebhook(msg *message.Message) error {
	// Acquire semaphore slot (blocks if at capacity)
	t.sem <- struct{}{}
	defer func() { <-t.sem }()

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

	if err := t.client.sendMessage(webhookMsg); err != nil {
		glog.Errorf("webhook target %q failed to send message for %s: %v", t.name, webhookMsg.Key, err)
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
				targetName := ""
				if msg.Metadata != nil {
					if keyValue, exists := msg.Metadata["key"]; exists {
						key = keyValue
					}
					targetName = msg.Metadata["target"]
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
				glog.Errorf("received dead letter message: %s, key: %s, target: %s", payload, key, targetName)
			case <-w.ctx.Done():
				return
			}
		}
	}()

	return nil
}
