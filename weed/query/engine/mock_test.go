package engine

import (
	"context"
	"testing"
)

func TestMockBrokerClient_BasicFunctionality(t *testing.T) {
	mockBroker := NewMockBrokerClient()

	// Test ListNamespaces
	namespaces, err := mockBroker.ListNamespaces(context.Background())
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(namespaces) != 2 {
		t.Errorf("Expected 2 namespaces, got %d", len(namespaces))
	}

	// Test ListTopics
	topics, err := mockBroker.ListTopics(context.Background(), "default")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(topics) != 2 {
		t.Errorf("Expected 2 topics in default namespace, got %d", len(topics))
	}

	// Test GetTopicSchema
	schema, keyColumns, _, err := mockBroker.GetTopicSchema(context.Background(), "default", "user_events")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if len(schema.Fields) != 3 {
		t.Errorf("Expected 3 fields in user_events schema, got %d", len(schema.Fields))
	}
	if len(keyColumns) == 0 {
		t.Error("Expected at least one key column")
	}
}

func TestMockBrokerClient_FailureScenarios(t *testing.T) {
	mockBroker := NewMockBrokerClient()

	// Configure mock to fail
	mockBroker.SetFailure(true, "simulated broker failure")

	// Test that operations fail as expected
	_, err := mockBroker.ListNamespaces(context.Background())
	if err == nil {
		t.Error("Expected error when mock is configured to fail")
	}

	_, err = mockBroker.ListTopics(context.Background(), "default")
	if err == nil {
		t.Error("Expected error when mock is configured to fail")
	}

	_, _, _, err = mockBroker.GetTopicSchema(context.Background(), "default", "user_events")
	if err == nil {
		t.Error("Expected error when mock is configured to fail")
	}

	// Test that filer client also fails
	_, err = mockBroker.GetFilerClient()
	if err == nil {
		t.Error("Expected error when mock is configured to fail")
	}

	// Reset mock to working state
	mockBroker.SetFailure(false, "")

	// Test that operations work again
	namespaces, err := mockBroker.ListNamespaces(context.Background())
	if err != nil {
		t.Errorf("Expected no error after resetting mock, got %v", err)
	}
	if len(namespaces) == 0 {
		t.Error("Expected namespaces after resetting mock")
	}
}

func TestMockBrokerClient_TopicManagement(t *testing.T) {
	mockBroker := NewMockBrokerClient()

	// Test ConfigureTopic (add a new topic)
	err := mockBroker.ConfigureTopic(context.Background(), "test", "new-topic", 1, nil, []string{})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the topic was added
	topics, err := mockBroker.ListTopics(context.Background(), "test")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	foundNewTopic := false
	for _, topic := range topics {
		if topic == "new-topic" {
			foundNewTopic = true
			break
		}
	}
	if !foundNewTopic {
		t.Error("Expected new-topic to be in the topics list")
	}

	// Test DeleteTopic
	err = mockBroker.DeleteTopic(context.Background(), "test", "new-topic")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the topic was removed
	topics, err = mockBroker.ListTopics(context.Background(), "test")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	for _, topic := range topics {
		if topic == "new-topic" {
			t.Error("Expected new-topic to be removed from topics list")
		}
	}
}

func TestSQLEngineWithMockBrokerClient_ErrorHandling(t *testing.T) {
	// Create an engine with a failing mock broker
	mockBroker := NewMockBrokerClient()
	mockBroker.SetFailure(true, "mock broker unavailable")

	catalog := &SchemaCatalog{
		databases:       make(map[string]*DatabaseInfo),
		currentDatabase: "default",
		brokerClient:    mockBroker,
	}

	engine := &SQLEngine{catalog: catalog}

	// Test that queries fail gracefully with proper error messages
	result, err := engine.ExecuteSQL(context.Background(), "SELECT * FROM nonexistent_topic")

	// ExecuteSQL itself should not return an error, but the result should contain an error
	if err != nil {
		// If ExecuteSQL returns an error, that's also acceptable for this test
		t.Logf("ExecuteSQL returned error (acceptable): %v", err)
		return
	}

	// Should have an error in the result when broker is unavailable
	if result.Error == nil {
		t.Error("Expected error in query result when broker is unavailable")
	} else {
		t.Logf("Got expected error in result: %v", result.Error)
	}
}
