package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

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
	payload := map[string]interface{}{
		"key":        message.Key,
		"event_type": message.EventType,
		"message":    message.MessageData,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, h.endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
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
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status code: %d", resp.StatusCode)
	}

	return nil
}
