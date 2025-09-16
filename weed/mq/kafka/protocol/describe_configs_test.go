package protocol

import (
	"encoding/binary"
	"testing"
)

func TestDescribeConfigs_ParseRequest(t *testing.T) {
	handler := &Handler{}

	// Build a test request with one topic resource
	request := make([]byte, 0, 256)

	// Resources count (1)
	resourcesCount := make([]byte, 4)
	binary.BigEndian.PutUint32(resourcesCount, 1)
	request = append(request, resourcesCount...)

	// Resource type (2 = topic)
	request = append(request, 2)

	// Resource name ("test-topic")
	topicName := "test-topic"
	nameLength := make([]byte, 2)
	binary.BigEndian.PutUint16(nameLength, uint16(len(topicName)))
	request = append(request, nameLength...)
	request = append(request, []byte(topicName)...)

	// Config names count (0 = return all configs)
	configNamesCount := make([]byte, 4)
	binary.BigEndian.PutUint32(configNamesCount, 0)
	request = append(request, configNamesCount...)

	// Parse the request
	resources, err := handler.parseDescribeConfigsRequest(request)
	if err != nil {
		t.Fatalf("Failed to parse request: %v", err)
	}

	if len(resources) != 1 {
		t.Fatalf("Expected 1 resource, got %d", len(resources))
	}

	resource := resources[0]
	if resource.ResourceType != 2 {
		t.Errorf("Expected resource type 2, got %d", resource.ResourceType)
	}

	if resource.ResourceName != topicName {
		t.Errorf("Expected resource name %s, got %s", topicName, resource.ResourceName)
	}

	if len(resource.ConfigNames) != 0 {
		t.Errorf("Expected 0 config names, got %d", len(resource.ConfigNames))
	}
}

func TestDescribeConfigs_TopicConfigs(t *testing.T) {
	handler := &Handler{}

	// Test getting all topic configs
	configs := handler.getTopicConfigs("test-topic", []string{})

	if len(configs) == 0 {
		t.Fatal("Expected some topic configs, got none")
	}

	// Check for essential configs
	expectedConfigs := []string{"cleanup.policy", "retention.ms", "retention.bytes", "max.message.bytes"}
	configMap := make(map[string]ConfigEntry)
	for _, config := range configs {
		configMap[config.Name] = config
	}

	for _, expectedConfig := range expectedConfigs {
		if _, exists := configMap[expectedConfig]; !exists {
			t.Errorf("Expected config %s not found", expectedConfig)
		}
	}

	// Test filtering specific configs
	requestedConfigs := []string{"cleanup.policy", "retention.ms"}
	filteredConfigs := handler.getTopicConfigs("test-topic", requestedConfigs)

	if len(filteredConfigs) != len(requestedConfigs) {
		t.Errorf("Expected %d filtered configs, got %d", len(requestedConfigs), len(filteredConfigs))
	}

	for _, config := range filteredConfigs {
		found := false
		for _, requested := range requestedConfigs {
			if config.Name == requested {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Unexpected config in filtered results: %s", config.Name)
		}
	}
}

func TestDescribeConfigs_BrokerConfigs(t *testing.T) {
	handler := &Handler{}

	// Test getting all broker configs
	configs := handler.getBrokerConfigs([]string{})

	if len(configs) == 0 {
		t.Fatal("Expected some broker configs, got none")
	}

	// Check for essential configs
	expectedConfigs := []string{"log.retention.hours", "log.segment.bytes", "num.network.threads"}
	configMap := make(map[string]ConfigEntry)
	for _, config := range configs {
		configMap[config.Name] = config
	}

	for _, expectedConfig := range expectedConfigs {
		if _, exists := configMap[expectedConfig]; !exists {
			t.Errorf("Expected config %s not found", expectedConfig)
		}
	}

	// Verify read-only flags are set correctly
	if config, exists := configMap["num.network.threads"]; exists {
		if !config.ReadOnly {
			t.Error("Expected num.network.threads to be read-only")
		}
	}
}

func TestDescribeConfigs_BuildConfigEntry(t *testing.T) {
	handler := &Handler{}

	config := ConfigEntry{
		Name:      "test.config",
		Value:     "test-value",
		ReadOnly:  true,
		IsDefault: false,
		Sensitive: false,
	}

	entry := handler.buildConfigEntry(config)

	if len(entry) == 0 {
		t.Fatal("Expected non-empty config entry")
	}

	// Verify the entry contains the config name and value
	// (Full parsing would be complex, so we just check it's not empty and reasonable size)
	expectedMinSize := len(config.Name) + len(config.Value) + 7 // name + value + flags + length fields
	if len(entry) < expectedMinSize {
		t.Errorf("Config entry too small: %d bytes, expected at least %d", len(entry), expectedMinSize)
	}

	t.Logf("Built config entry: %d bytes for config %s", len(entry), config.Name)
}

func TestDescribeConfigs_EndToEnd(t *testing.T) {
	handler := &Handler{}

	// Build a complete DescribeConfigs request
	request := make([]byte, 0, 256)

	// Resources count (2: one topic, one broker)
	resourcesCount := make([]byte, 4)
	binary.BigEndian.PutUint32(resourcesCount, 2)
	request = append(request, resourcesCount...)

	// Resource 1: Topic
	request = append(request, 2) // Resource type = topic
	topicName := "my-topic"
	nameLength := make([]byte, 2)
	binary.BigEndian.PutUint16(nameLength, uint16(len(topicName)))
	request = append(request, nameLength...)
	request = append(request, []byte(topicName)...)
	configNamesCount := make([]byte, 4)
	binary.BigEndian.PutUint32(configNamesCount, 0) // All configs
	request = append(request, configNamesCount...)

	// Resource 2: Broker
	request = append(request, 4) // Resource type = broker
	brokerName := "1"
	nameLength2 := make([]byte, 2)
	binary.BigEndian.PutUint16(nameLength2, uint16(len(brokerName)))
	request = append(request, nameLength2...)
	request = append(request, []byte(brokerName)...)
	configNamesCount2 := make([]byte, 4)
	binary.BigEndian.PutUint32(configNamesCount2, 0) // All configs
	request = append(request, configNamesCount2...)

	// Process the request
	response, err := handler.handleDescribeConfigs(12345, 0, request)
	if err != nil {
		t.Fatalf("Failed to handle DescribeConfigs: %v", err)
	}

	if len(response) == 0 {
		t.Fatal("Expected non-empty response")
	}

	// Basic validation - response should contain correlation ID and some data
	if len(response) < 12 { // correlation ID (4) + throttle time (4) + resources count (4)
		t.Errorf("Response too short: %d bytes", len(response))
	}

	// Check correlation ID
	correlationID := binary.BigEndian.Uint32(response[0:4])
	if correlationID != 12345 {
		t.Errorf("Expected correlation ID 12345, got %d", correlationID)
	}

	t.Logf("DescribeConfigs response: %d bytes", len(response))
}

func TestDescribeConfigs_ConfigValues(t *testing.T) {
	handler := &Handler{}

	// Test that config values are reasonable
	topicConfigs := handler.getTopicConfigs("test-topic", []string{})

	for _, config := range topicConfigs {
		if config.Name == "" {
			t.Error("Config name should not be empty")
		}

		if config.Value == "" {
			t.Errorf("Config value should not be empty for %s", config.Name)
		}

		// Test specific values
		switch config.Name {
		case "cleanup.policy":
			if config.Value != "delete" {
				t.Errorf("Expected cleanup.policy=delete, got %s", config.Value)
			}
		case "retention.ms":
			if config.Value != "604800000" {
				t.Errorf("Expected retention.ms=604800000, got %s", config.Value)
			}
		case "min.insync.replicas":
			if config.Value != "1" {
				t.Errorf("Expected min.insync.replicas=1, got %s", config.Value)
			}
		}

		t.Logf("Config: %s = %s (readonly=%v, default=%v, sensitive=%v)",
			config.Name, config.Value, config.ReadOnly, config.IsDefault, config.Sensitive)
	}
}
