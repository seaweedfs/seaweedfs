package webhook

import (
	"fmt"
	"net/url"
	"slices"
	"strconv"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

const (
	queueName       = "webhook"
	pubSubTopicName = "webhook_topic"
	deadLetterTopic = "webhook_dead_letter"
)

type eventType string

const (
	eventTypeCreate eventType = "create"
	eventTypeDelete eventType = "delete"
	eventTypeUpdate eventType = "update"
	eventTypeRename eventType = "rename"
)

func (e eventType) valid() bool {
	return slices.Contains([]eventType{
		eventTypeCreate,
		eventTypeDelete,
		eventTypeUpdate,
		eventTypeRename,
	},
		e,
	)
}

var (
	pubSubHandlerNameTemplate = func(n int) string {
		return "webhook_handler_" + strconv.Itoa(n)
	}
)

type client interface {
	sendMessage(message *webhookMessage) error
}

type webhookMessage struct {
	Key          string                      `json:"key"`
	EventType    string                      `json:"event_type"`
	Notification *filer_pb.EventNotification `json:"message_data"`
}

func newWebhookMessage(key string, message proto.Message) *webhookMessage {
	notification, ok := message.(*filer_pb.EventNotification)
	if !ok {
		return nil
	}

	eventType := string(detectEventType(notification))

	return &webhookMessage{
		Key:          key,
		EventType:    eventType,
		Notification: notification,
	}
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

	eventTypes   []string
	pathPrefixes []string
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
	if maxBackoffSeconds := configuration.GetInt(prefix + "max_backoff_seconds"); maxBackoffSeconds > 0 {
		c.maxBackoffSeconds = maxBackoffSeconds
	}
	if timeout := configuration.GetInt(prefix + "timeout_seconds"); timeout > 0 {
		c.timeoutSeconds = timeout
	}

	c.eventTypes = configuration.GetStringSlice(prefix + "event_types")
	c.pathPrefixes = configuration.GetStringSlice(prefix + "path_prefixes")

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

func detectEventType(notification *filer_pb.EventNotification) eventType {
	hasOldEntry := notification.OldEntry != nil
	hasNewEntry := notification.NewEntry != nil
	hasNewParentPath := notification.NewParentPath != ""

	if !hasOldEntry && hasNewEntry {
		return eventTypeCreate
	}

	if hasOldEntry && !hasNewEntry {
		return eventTypeDelete
	}

	if hasOldEntry && hasNewEntry {
		if hasNewParentPath {
			return eventTypeRename
		}

		return eventTypeUpdate
	}

	return eventTypeUpdate
}
