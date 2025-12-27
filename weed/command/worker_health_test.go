package command

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/worker"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"github.com/stretchr/testify/assert"
)

type mockAdminClient struct {
	connected bool
}

func (m *mockAdminClient) Connect() error {
	return nil
}

func (m *mockAdminClient) Disconnect() error {
	return nil
}

func (m *mockAdminClient) RegisterWorker(w *types.WorkerData) error {
	return nil
}

func (m *mockAdminClient) SendHeartbeat(workerID string, status *types.WorkerStatus) error {
	return nil
}

func (m *mockAdminClient) RequestTask(workerID string, capabilities []types.TaskType) (*types.TaskInput, error) {
	return nil, nil
}

func (m *mockAdminClient) CompleteTask(taskID string, success bool, errorMsg string) error {
	return nil
}

func (m *mockAdminClient) CompleteTaskWithMetadata(taskID string, success bool, errorMsg string, metadata map[string]string) error {
	return nil
}

func (m *mockAdminClient) UpdateTaskProgress(taskID string, progress float64) error {
	return nil
}

func (m *mockAdminClient) IsConnected() bool {
	return m.connected
}

func TestWorkerHealthHandler(t *testing.T) {
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	workerHealthHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Server"), "SeaweedFS Worker")
}

func TestWorkerReadyHandler(t *testing.T) {
	testCases := []struct {
		name           string
		admin          worker.AdminClient
		expectedStatus int
	}{
		{
			name:           "connected admin",
			admin:          &mockAdminClient{connected: true},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "disconnected admin",
			admin:          &mockAdminClient{connected: false},
			expectedStatus: http.StatusServiceUnavailable,
		},
		{
			name:           "nil admin",
			admin:          nil,
			expectedStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := &types.WorkerConfig{
				AdminServer:   "localhost:9333",
				Capabilities:  []types.TaskType{types.TaskTypeVacuum},
				MaxConcurrent: 3,
			}
			w, err := worker.NewWorker(config)
			assert.NoError(t, err)

			if tc.admin != nil {
				w.SetAdminClient(tc.admin)
			}

			handler := workerReadyHandler(w)

			req := httptest.NewRequest("GET", "/ready", nil)
			rec := httptest.NewRecorder()

			handler(rec, req)

			assert.Equal(t, tc.expectedStatus, rec.Code)
			assert.Contains(t, rec.Header().Get("Server"), "SeaweedFS Worker")
		})
	}
}
