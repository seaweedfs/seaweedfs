package webhook

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

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
	client      client
	config      *config
	bufferChan  chan *webhookMessage
	retryChan   chan *retryMessage
	worker      errgroup.Group
	retryWorker errgroup.Group
	ctx         context.Context
	shutdown    atomic.Bool
}

type webhookMessage struct {
	Key     string        `json:"key"`
	Message proto.Message `json:"message"`
}

type retryMessage struct {
	*webhookMessage
	attempts      int
	nextRetryTime time.Time
}
type config struct {
	endpoint        string
	authBearerToken string
	timeoutSeconds  int

	maxRetries        int
	backoffSeconds    int
	maxBackoffSeconds int
	nWorkers          int
	nRetryWorkers     int
	bufferSize        int
}

func newConfigWithDefaults(configuration util.Configuration, prefix string) *config {
	c := &config{
		endpoint:          configuration.GetString(prefix + "endpoint"),
		authBearerToken:   configuration.GetString(prefix + "bearer_token"),
		timeoutSeconds:    configuration.GetInt(prefix + "timeout_seconds"),
		maxRetries:        configuration.GetInt(prefix + "max_retries"),
		backoffSeconds:    configuration.GetInt(prefix + "backoff_seconds"),
		maxBackoffSeconds: configuration.GetInt(prefix + "max_backoff_seconds"),
		nWorkers:          configuration.GetInt(prefix + "workers"),
		nRetryWorkers:     configuration.GetInt(prefix + "retry_workers"),
		bufferSize:        configuration.GetInt(prefix + "buffer_size"),
	}
	if c.nWorkers <= 0 {
		c.nWorkers = 20
	}
	if c.nRetryWorkers <= 0 {
		c.nRetryWorkers = 5
	}
	if c.bufferSize <= 0 {
		c.bufferSize = 1000
	}
	if c.backoffSeconds <= 0 {
		c.backoffSeconds = 5
	}
	if c.maxBackoffSeconds <= 0 {
		c.maxBackoffSeconds = 300
	}
	if c.timeoutSeconds <= 0 {
		c.timeoutSeconds = 30
	}

	return c
}

func (c *config) validate() error {
	_, err := url.Parse(c.endpoint)
	if err != nil {
		return fmt.Errorf("invalid webhook endpoint %w", err)
	}

	if c.timeoutSeconds > 60 {
		return fmt.Errorf("invalid webhook timeout %w", fmt.Errorf("timeout too large"))
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
	w.bufferChan = make(chan *webhookMessage, cfg.bufferSize)
	w.retryChan = make(chan *retryMessage, 100)
	w.shutdown.Store(false)

	client, err := newHTTPClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create webhook client: %v", err)
	}
	w.client = client

	go w.setupGracefulShutdown()
	go w.startWorker()
	go w.startRetryWorker()

	return nil
}

func (w *WebhookQueue) setupGracefulShutdown() {
	ctx, cancel := context.WithCancel(context.Background())
	w.ctx = ctx

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		w.shutdown.Store(true)
		glog.Infof("webhook queue received shutdown signal")
		w.drain()

		cancel()
	}()
}

func (w *WebhookQueue) SendMessage(key string, message proto.Message) error {
	if w.client == nil {
		return fmt.Errorf("webhook client not initialized")
	}

	if w.shutdown.Load() {
		return nil
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
						if w.config.maxRetries > 0 {
							retryMsg := &retryMessage{
								webhookMessage: message,
								attempts:       1,
								nextRetryTime:  time.Now().Add(w.calculateBackoff(1)),
							}
							select {
							case w.retryChan <- retryMsg:
							default:
								glog.Warningf("retry queue full, dropping message %s", message.Key)
							}
						} else {
							glog.Errorf("failed to send message to webhook %s: %v", message.Key, err)
						}
					}
				}
			}
		})
	}

	if err := w.worker.Wait(); err != nil {
		glog.Errorf("webhook worker exited with error: %v", err)
	}
}

func (w *WebhookQueue) startRetryWorker() {
	for i := 0; i < w.config.nRetryWorkers; i++ {
		w.retryWorker.Go(func() error {
			for {
				select {
				case <-w.ctx.Done():
					return w.ctx.Err()
				case msg := <-w.retryChan:
					waitTime := time.Until(msg.nextRetryTime)
					if waitTime > 0 {
						select {
						case <-time.After(waitTime):
						case <-w.ctx.Done():
							return w.ctx.Err()
						}
					}

					if err := w.client.sendMessage(msg.webhookMessage); err != nil {
						msg.attempts++
						if msg.attempts < w.config.maxRetries {
							backoffDuration := w.calculateBackoff(msg.attempts)
							msg.nextRetryTime = time.Now().Add(backoffDuration)
							select {
							case w.retryChan <- msg:
							default:
								glog.Warningf("retry queue full, dropping message %s after %d attempts", msg.Key, msg.attempts)
							}
						} else {
							glog.Errorf("webhook message %s failed after %d attempts: %v", msg.Key, msg.attempts, err)
						}
					}
				}
			}
		})
	}

	if err := w.retryWorker.Wait(); err != nil {
		glog.Errorf("webhook retry worker exited with error: %v", err)
	}
}

func (w *WebhookQueue) calculateBackoff(attempt int) time.Duration {
	base := float64(w.config.backoffSeconds)
	expBackoff := base * math.Pow(2, float64(attempt-1))
	jitter := rand.Float64() * 0.3 * expBackoff
	totalSeconds := expBackoff + jitter

	if totalSeconds > float64(w.config.maxBackoffSeconds) {
		totalSeconds = float64(w.config.maxBackoffSeconds)
	}

	return time.Duration(totalSeconds) * time.Second
}

func (w *WebhookQueue) drain() {
	if len(w.bufferChan) == 0 && len(w.retryChan) == 0 {
		return
	}

	glog.Infof("draining %d messages and %d retries", len(w.bufferChan), len(w.retryChan))

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for {
		select {
		case <-ticker.C:
			if len(w.bufferChan) == 0 && len(w.retryChan) == 0 {
				glog.Infof("webhook queue drained successfully")
				return
			}
		case <-timeoutCtx.Done():
			remaining := len(w.bufferChan) + len(w.retryChan)
			if remaining > 0 {
				glog.Warningf("webhook drain timeout, %d messages may be lost", remaining)
			}
			return
		}
	}
}
