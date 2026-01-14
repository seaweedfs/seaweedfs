package telemetry

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/telemetry/proto"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	protobuf "google.golang.org/protobuf/proto"
)

type Client struct {
	url        string
	enabled    bool
	instanceID string
	httpClient *http.Client
	clusterId  string
}

// NewClient creates a new telemetry client
func NewClient(url string, enabled bool) *Client {
	return &Client{
		url:        url,
		enabled:    enabled,
		instanceID: uuid.New().String(), // Generate UUID in memory only
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) SetClusterId(clusterId string) {
	c.clusterId = clusterId
}

// IsEnabled returns whether telemetry is enabled
func (c *Client) IsEnabled() bool {
	return c.enabled && c.url != ""
}

// SendTelemetry sends telemetry data synchronously using protobuf format
func (c *Client) SendTelemetry(data *proto.TelemetryData) error {
	if !c.IsEnabled() {
		return nil
	}

	// Set the cluster ID
	if c.clusterId != "" {
		data.ClusterId = c.clusterId
	}

	return c.sendProtobuf(data)
}

// SendTelemetryAsync sends telemetry data asynchronously
func (c *Client) SendTelemetryAsync(data *proto.TelemetryData) {
	if !c.IsEnabled() {
		return
	}

	go func() {
		if err := c.SendTelemetry(data); err != nil {
			glog.V(1).Infof("Failed to send telemetry: %v", err)
		}
	}()
}

// sendProtobuf sends data using protobuf format
func (c *Client) sendProtobuf(data *proto.TelemetryData) error {
	req := &proto.TelemetryRequest{
		Data: data,
	}

	body, err := protobuf.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf: %w", err)
	}

	httpReq, err := http.NewRequest("POST", c.url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", fmt.Sprintf("SeaweedFS/%s", data.Version))

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned status %d", resp.StatusCode)
	}

	glog.V(2).Infof("Telemetry sent successfully via protobuf")
	return nil
}

// GetInstanceID returns the current instance ID
func (c *Client) GetInstanceID() string {
	return c.instanceID
}
