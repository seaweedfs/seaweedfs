package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

const (
	maxWebhookRetryDepth = 2
)

type httpClient struct {
	endpoint   string
	token      string
	timeout    time.Duration
	client     *http.Client // Reused HTTP client with redirect prevention
	endpointMu sync.RWMutex
	finalURL   string // Cached final URL after following redirects
}

func newHTTPClient(cfg *config) (*httpClient, error) {
	return &httpClient{
		endpoint: cfg.endpoint,
		token:    cfg.authBearerToken,
		timeout:  time.Duration(cfg.timeoutSeconds) * time.Second,
		client: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (h *httpClient) sendMessage(message *webhookMessage) error {
	return h.sendMessageWithRetry(message, 0)
}

func (h *httpClient) sendMessageWithRetry(message *webhookMessage, depth int) error {
	// Prevent infinite recursion
	if depth > maxWebhookRetryDepth {
		return fmt.Errorf("webhook max retry depth exceeded")
	}

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

	// Use cached final URL if available, otherwise use original endpoint
	h.endpointMu.RLock()
	targetURL := h.finalURL
	usingCachedURL := targetURL != ""
	if targetURL == "" {
		targetURL = h.endpoint
	}
	h.endpointMu.RUnlock()

	req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}

	// Apply timeout via context (not on client) to avoid redundancy
	if h.timeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
		defer cancel()
		req = req.WithContext(ctx)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		if drainErr := drainResponse(resp); drainErr != nil {
			glog.Errorf("failed to drain response: %v", drainErr)
		}

		// If using cached URL and request failed, clear cache and retry with original endpoint
		if usingCachedURL && depth == 0 {
			glog.V(1).Infof("Webhook request to cached URL %s failed, clearing cache and retrying with original endpoint", targetURL)
			h.setFinalURL("")
			return h.sendMessageWithRetry(message, depth+1)
		}

		return fmt.Errorf("failed to send request: %w", err)
	}

	// Handle redirects by caching the final destination and recreating POST request
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		// Drain and close response body to enable connection reuse
		util_http.CloseResponse(resp)

		location := resp.Header.Get("Location")
		if location == "" {
			return fmt.Errorf("webhook returned redirect status %d without Location header", resp.StatusCode)
		}

		// Resolve relative URLs against the request URL
		reqURL := req.URL
		finalURL, err := reqURL.Parse(location)
		if err != nil {
			return fmt.Errorf("failed to parse redirect location: %w", err)
		}

		// Update finalURL to follow the redirect for this attempt
		finalURLStr := finalURL.String()
		h.setFinalURL(finalURLStr)

		if depth == 0 {
			glog.V(1).Infof("Webhook endpoint redirected from %s to %s, caching final destination", targetURL, finalURLStr)
		} else {
			glog.V(1).Infof("Webhook endpoint redirected from %s to %s (following redirect on retry)", targetURL, finalURLStr)
		}

		// Recreate the POST request to the final destination (increment depth to prevent infinite loops)
		return h.sendMessageWithRetry(message, depth+1)
	}

	// If using cached URL and got an error response, clear cache and retry with original endpoint
	if resp.StatusCode >= 400 && usingCachedURL && depth == 0 {
		// Drain and close response body to enable connection reuse
		util_http.CloseResponse(resp)

		glog.V(1).Infof("Webhook request to cached URL %s returned error %d, clearing cache and retrying with original endpoint", targetURL, resp.StatusCode)
		h.setFinalURL("")
		return h.sendMessageWithRetry(message, depth+1)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Drain and close response body before returning error to enable connection reuse
		util_http.CloseResponse(resp)
		return fmt.Errorf("webhook returned status code: %d", resp.StatusCode)
	}

	// Drain and close response body on success to enable connection reuse
	util_http.CloseResponse(resp)

	return nil
}

func (h *httpClient) setFinalURL(url string) {
	h.endpointMu.Lock()
	h.finalURL = url
	h.endpointMu.Unlock()
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
