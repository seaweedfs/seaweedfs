package webhook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"google.golang.org/protobuf/proto"
)

type httpClient struct {
	endpoint string
	token    string
}

func newHTTPClient(cfg *config) (*httpClient, error) {
	return &httpClient{
		endpoint: cfg.endpoint,
		token:    cfg.authBearerToken,
	}, nil
}

func (h *httpClient) sendMessage(key string, message proto.Message) error {
	payload := map[string]interface{}{
		"key":     key,
		"message": message,
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
