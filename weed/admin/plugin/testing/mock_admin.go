package testing

import (
	"io"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/plugin_pb"
)

// MockAdminServer implements the PluginService for testing
type MockAdminServer struct {
	mu                     sync.RWMutex
	receivedPluginMessages []*plugin_pb.PluginMessage
	receivedJobMessages    []*plugin_pb.JobExecutionMessage
	jobResponses           map[string][]*plugin_pb.JobProgressMessage
	pluginResponses        map[string][]*plugin_pb.AdminMessage
	streams                map[string]*MockStream
	closed                 bool
}

// MockStream represents a bidirectional stream
type MockStream struct {
	mu       sync.RWMutex
	messages chan interface{}
	closed   bool
}

// NewMockAdminServer creates a new mock admin server
func NewMockAdminServer() *MockAdminServer {
	return &MockAdminServer{
		receivedPluginMessages: make([]*plugin_pb.PluginMessage, 0),
		receivedJobMessages:    make([]*plugin_pb.JobExecutionMessage, 0),
		jobResponses:           make(map[string][]*plugin_pb.JobProgressMessage),
		pluginResponses:        make(map[string][]*plugin_pb.AdminMessage),
		streams:                make(map[string]*MockStream),
	}
}

// Connect implements the Connect RPC - bidirectional stream for plugin registration
func (m *MockAdminServer) Connect(stream plugin_pb.PluginService_ConnectServer) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return io.EOF
	}
	m.mu.Unlock()

	// Receive messages from plugin
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		m.mu.Lock()
		m.receivedPluginMessages = append(m.receivedPluginMessages, msg)
		m.mu.Unlock()

		// Send response if available
		var response *plugin_pb.AdminMessage
		if msg.GetRegister() != nil {
			response = &plugin_pb.AdminMessage{
				Content: &plugin_pb.AdminMessage_ConfigUpdate{
					ConfigUpdate: &plugin_pb.ConfigUpdate{
						JobType: "test_job_type",
					},
				},
			}
		}

		if response != nil {
			err = stream.Send(response)
			if err != nil {
				return err
			}
		}
	}
}

// ExecuteJob implements the ExecuteJob RPC - bidirectional stream for job execution
func (m *MockAdminServer) ExecuteJob(stream plugin_pb.PluginService_ExecuteJobServer) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return io.EOF
	}
	m.mu.Unlock()

	// Receive execution messages from plugin
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		m.mu.Lock()
		m.receivedJobMessages = append(m.receivedJobMessages, msg)
		m.mu.Unlock()

		// Send progress update if available
		jobID := msg.JobId
		m.mu.RLock()
		responses, ok := m.jobResponses[jobID]
		m.mu.RUnlock()

		if ok && len(responses) > 0 {
			response := responses[0]
			err = stream.Send(response)
			if err != nil {
				return err
			}

			m.mu.Lock()
			m.jobResponses[jobID] = responses[1:]
			m.mu.Unlock()
		}
	}
}

// AddJobResponse adds a pre-configured response for a specific job
func (m *MockAdminServer) AddJobResponse(jobID string, response *plugin_pb.JobProgressMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.jobResponses[jobID] = append(m.jobResponses[jobID], response)
}

// AddPluginResponse adds a pre-configured response for plugin messages
func (m *MockAdminServer) AddPluginResponse(response *plugin_pb.AdminMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Store responses indexed by a generic key for now
	key := "default"
	m.pluginResponses[key] = append(m.pluginResponses[key], response)
}

// GetReceivedPluginMessages returns all received plugin messages
func (m *MockAdminServer) GetReceivedPluginMessages() []*plugin_pb.PluginMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy
	result := make([]*plugin_pb.PluginMessage, len(m.receivedPluginMessages))
	copy(result, m.receivedPluginMessages)
	return result
}

// GetReceivedJobMessages returns all received job execution messages
func (m *MockAdminServer) GetReceivedJobMessages() []*plugin_pb.JobExecutionMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy
	result := make([]*plugin_pb.JobExecutionMessage, len(m.receivedJobMessages))
	copy(result, m.receivedJobMessages)
	return result
}

// GetReceivedJobMessages returns job execution messages filtered by job ID
func (m *MockAdminServer) GetJobMessages(jobID string) []*plugin_pb.JobExecutionMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*plugin_pb.JobExecutionMessage
	for _, msg := range m.receivedJobMessages {
		if msg.JobId == jobID {
			result = append(result, msg)
		}
	}
	return result
}

// CountReceivedMessages returns the count of received plugin messages
func (m *MockAdminServer) CountReceivedMessages() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.receivedPluginMessages)
}

// CountReceivedJobMessages returns the count of received job execution messages
func (m *MockAdminServer) CountReceivedJobMessages() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.receivedJobMessages)
}

// ClearMessages clears all recorded messages
func (m *MockAdminServer) ClearMessages() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.receivedPluginMessages = make([]*plugin_pb.PluginMessage, 0)
	m.receivedJobMessages = make([]*plugin_pb.JobExecutionMessage, 0)
}

// Close closes the mock server
func (m *MockAdminServer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}

// IsClosed returns whether the server is closed
func (m *MockAdminServer) IsClosed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.closed
}

// VerifyPluginRegistration verifies that a plugin registration was received
func (m *MockAdminServer) VerifyPluginRegistration(pluginID string) error {
	messages := m.GetReceivedPluginMessages()
	for _, msg := range messages {
		if reg := msg.GetRegister(); reg != nil && reg.PluginId == pluginID {
			return nil
		}
	}
	return io.EOF
}

// VerifyJobExecution verifies that job execution messages were received for a job
func (m *MockAdminServer) VerifyJobExecution(jobID string) error {
	messages := m.GetJobMessages(jobID)
	if len(messages) == 0 {
		return io.EOF
	}
	return nil
}

// VerifyJobCompletion verifies that a job completion message was received
func (m *MockAdminServer) VerifyJobCompletion(jobID string) error {
	messages := m.GetJobMessages(jobID)
	for _, msg := range messages {
		if msg.GetJobCompleted() != nil {
			return nil
		}
	}
	return io.EOF
}

// VerifyJobFailure verifies that a job failure message was received
func (m *MockAdminServer) VerifyJobFailure(jobID string) error {
	messages := m.GetJobMessages(jobID)
	for _, msg := range messages {
		if msg.GetJobFailed() != nil {
			return nil
		}
	}
	return io.EOF
}
