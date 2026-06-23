package protocol

import (
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/integration"
)

// autoCreateMockHandler models the flaky-existence scenario: TopicExists reports
// the topic missing (a transient broker/filer false-negative), while a create
// attempt reports the topic already exists. It reuses FastMockHandler for the
// rest of the interface and overrides only the two methods under test.
type autoCreateMockHandler struct {
	*FastMockHandler
	exists    bool
	createErr error
}

func (h *autoCreateMockHandler) TopicExists(name string) bool { return h.exists }

func (h *autoCreateMockHandler) CreateTopic(name string, partitions int32) error {
	return h.createErr
}

// TestEnsureTopicExistsTreatsAlreadyExistsAsSuccess reproduces the offset-resumption
// flake: TopicExists returns a false-negative for a topic that is in fact present,
// so the auto-create attempt fails with ErrTopicAlreadyExists. The topic must still
// be reported as existing, otherwise the client sees UNKNOWN_TOPIC_OR_PARTITION.
func TestEnsureTopicExistsTreatsAlreadyExistsAsSuccess(t *testing.T) {
	handler := &Handler{
		seaweedMQHandler: &autoCreateMockHandler{
			FastMockHandler: &FastMockHandler{},
			exists:          false,
			createErr:       fmt.Errorf("%s: %w", "offset-resumption", integration.ErrTopicAlreadyExists),
		},
	}

	if !handler.ensureTopicExists("offset-resumption", 1) {
		t.Fatal("ensureTopicExists must report true when the create attempt finds the topic already exists")
	}
}

// TestEnsureTopicExistsReportsGenuineFailure confirms a real create failure (not
// "already exists") still results in the topic being treated as absent.
func TestEnsureTopicExistsReportsGenuineFailure(t *testing.T) {
	handler := &Handler{
		seaweedMQHandler: &autoCreateMockHandler{
			FastMockHandler: &FastMockHandler{},
			exists:          false,
			createErr:       fmt.Errorf("broker unavailable"),
		},
	}

	if handler.ensureTopicExists("missing-topic", 1) {
		t.Fatal("ensureTopicExists must report false when the topic is absent and creation genuinely fails")
	}
}

// TestEnsureTopicExistsWhenPresent covers the common path where the topic is
// already known to exist and no create attempt is needed.
func TestEnsureTopicExistsWhenPresent(t *testing.T) {
	handler := &Handler{
		seaweedMQHandler: &autoCreateMockHandler{
			FastMockHandler: &FastMockHandler{},
			exists:          true,
			createErr:       fmt.Errorf("CreateTopic should not be called when the topic exists"),
		},
	}

	if !handler.ensureTopicExists("present-topic", 1) {
		t.Fatal("ensureTopicExists must report true for an existing topic")
	}
}
