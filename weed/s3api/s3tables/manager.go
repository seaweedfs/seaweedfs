package s3tables

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// Manager provides reusable S3 Tables operations for shell/admin without HTTP routing.
type Manager struct {
	handler *S3TablesHandler
}

// NewManager creates a new Manager.
func NewManager() *Manager {
	return &Manager{handler: NewS3TablesHandler()}
}

// SetRegion sets the AWS region for ARN generation.
func (m *Manager) SetRegion(region string) {
	m.handler.SetRegion(region)
}

// SetAccountID sets the AWS account ID for ARN generation.
func (m *Manager) SetAccountID(accountID string) {
	m.handler.SetAccountID(accountID)
}

// Execute runs an S3 Tables operation and decodes the response into resp (if provided).
func (m *Manager) Execute(ctx context.Context, filerClient FilerClient, operation string, req interface{}, resp interface{}, identity string) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "/", bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	httpReq.Header.Set("X-Amz-Target", "S3Tables."+operation)
	if identity != "" {
		httpReq.Header.Set(s3_constants.AmzAccountId, identity)
		httpReq = httpReq.WithContext(s3_constants.SetIdentityNameInContext(httpReq.Context(), identity))
	}
	recorder := httptest.NewRecorder()
	m.handler.HandleRequest(recorder, httpReq, filerClient)
	return decodeS3TablesHTTPResponse(recorder, resp)
}

func decodeS3TablesHTTPResponse(recorder *httptest.ResponseRecorder, resp interface{}) error {
	result := recorder.Result()
	defer result.Body.Close()
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return err
	}
	if result.StatusCode >= http.StatusBadRequest {
		var errResp S3TablesError
		if len(data) > 0 {
			if jsonErr := json.Unmarshal(data, &errResp); jsonErr == nil && errResp.Message != "" {
				return &errResp
			}
		}
		return &S3TablesError{Type: ErrCodeInternalError, Message: string(bytes.TrimSpace(data))}
	}
	if resp == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, resp); err != nil {
		return err
	}
	return nil
}

// ManagerClient adapts a SeaweedFilerClient to the FilerClient interface.
type ManagerClient struct {
	client filer_pb.SeaweedFilerClient
}

// NewManagerClient wraps a filer client.
func NewManagerClient(client filer_pb.SeaweedFilerClient) *ManagerClient {
	return &ManagerClient{client: client}
}

// WithFilerClient implements FilerClient.
func (m *ManagerClient) WithFilerClient(streamingMode bool, fn func(client filer_pb.SeaweedFilerClient) error) error {
	if m.client == nil {
		return errors.New("nil filer client")
	}
	return fn(m.client)
}
