package blockapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client is a Go HTTP client for the master's block volume REST API.
// It supports multiple master addresses for failover.
type Client struct {
	Masters    []string
	HTTPClient *http.Client
}

// NewClient creates a Client from a comma-separated list of master URLs.
// Example: "http://m1:9333,http://m2:9333"
func NewClient(masters string) *Client {
	var addrs []string
	for _, m := range strings.Split(masters, ",") {
		m = strings.TrimSpace(m)
		if m != "" {
			addrs = append(addrs, m)
		}
	}
	return &Client{
		Masters: addrs,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// CreateVolume creates a new block volume.
func (c *Client) CreateVolume(ctx context.Context, req CreateVolumeRequest) (*VolumeInfo, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	resp, err := c.doRequest(ctx, http.MethodPost, "/block/volume", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, http.StatusOK, http.StatusCreated); err != nil {
		return nil, err
	}
	var info VolumeInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &info, nil
}

// DeleteVolume deletes a block volume by name.
func (c *Client) DeleteVolume(ctx context.Context, name string) error {
	resp, err := c.doRequest(ctx, http.MethodDelete, "/block/volume/"+name, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkStatus(resp, http.StatusOK)
}

// LookupVolume looks up a single block volume by name.
func (c *Client) LookupVolume(ctx context.Context, name string) (*VolumeInfo, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/block/volume/"+name, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, http.StatusOK); err != nil {
		return nil, err
	}
	var info VolumeInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &info, nil
}

// ListVolumes lists all block volumes.
func (c *Client) ListVolumes(ctx context.Context) ([]VolumeInfo, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/block/volumes", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, http.StatusOK); err != nil {
		return nil, err
	}
	var infos []VolumeInfo
	if err := json.NewDecoder(resp.Body).Decode(&infos); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return infos, nil
}

// AssignRole enqueues a role assignment for a block volume.
func (c *Client) AssignRole(ctx context.Context, req AssignRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	resp, err := c.doRequest(ctx, http.MethodPost, "/block/assign", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return checkStatus(resp, http.StatusOK)
}

// ExpandVolume expands a block volume to a new size.
func (c *Client) ExpandVolume(ctx context.Context, name string, newSizeBytes uint64) (uint64, error) {
	body, err := json.Marshal(ExpandVolumeRequest{NewSizeBytes: newSizeBytes})
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	resp, err := c.doRequest(ctx, http.MethodPost, "/block/volume/"+name+"/expand", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, http.StatusOK); err != nil {
		return 0, err
	}
	var out ExpandVolumeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("decode response: %w", err)
	}
	return out.CapacityBytes, nil
}

// ListServers lists all block-capable volume servers.
func (c *Client) ListServers(ctx context.Context) ([]ServerInfo, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/block/servers", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := checkStatus(resp, http.StatusOK); err != nil {
		return nil, err
	}
	var infos []ServerInfo
	if err := json.NewDecoder(resp.Body).Decode(&infos); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return infos, nil
}

// doRequest tries each master in order until one responds.
func (c *Client) doRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	var lastErr error
	for _, master := range c.Masters {
		url := strings.TrimRight(master, "/") + path
		var bodyReader io.Reader
		if body != nil {
			// Re-read body for retries: buffer it
			if lastErr != nil {
				// body was already consumed on previous attempt
				// We need a seekable body — caller should pass bytes.Reader
				if seeker, ok := body.(io.Seeker); ok {
					seeker.Seek(0, io.SeekStart)
				}
			}
			bodyReader = body
		}
		req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
		if err != nil {
			lastErr = fmt.Errorf("master %s: %w", master, err)
			continue
		}
		if method == http.MethodPost || method == http.MethodPut {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := c.HTTPClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("master %s: %w", master, err)
			continue
		}
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no master addresses configured")
}

// checkStatus returns an error if the response status is not in the accepted list.
func checkStatus(resp *http.Response, accepted ...int) error {
	for _, code := range accepted {
		if resp.StatusCode == code {
			return nil
		}
	}
	body, _ := io.ReadAll(resp.Body)
	// Try to extract error message from JSON.
	var errResp struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &errResp) == nil && errResp.Error != "" {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, errResp.Error)
	}
	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
}
