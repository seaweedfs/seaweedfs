package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

type httpClient struct {
	endpoint string
	token    string
	timeout  time.Duration
}

func newHTTPClient(cfg *config) (*httpClient, error) {
	return &httpClient{
		endpoint: cfg.endpoint,
		token:    cfg.authBearerToken,
		timeout:  time.Duration(cfg.timeoutSeconds) * time.Second,
	}, nil
}

func (h *httpClient) sendMessage(message *webhookMessage) error {
	// Serialize the protobuf message to JSON for HTTP payload
	notificationData, err := json.Marshal(message.Notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	payload := map[string]interface{}{
		"key":        message.Key,
		"event_type": message.EventType,
		"message":    json.RawMessage(notificationData),
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, h.endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}

	if h.timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
		defer cancel()
		req = req.WithContext(ctx)
	}

	resp, err := util_http.Do(req)
	if err != nil {
		if err = drainResponse(resp); err != nil {
			glog.Errorf("failed to drain response: %v", err)
		}

		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status code: %d", resp.StatusCode)
	}

	return nil
}

func drainResponse(resp *http.Response) error {
	if resp == nil || resp.Body == nil {
		return nil
	}

	_, err := io.ReadAll(resp.Body)

	return errors.Join(
		err,
		resp.Body.Close(),
	)
}
