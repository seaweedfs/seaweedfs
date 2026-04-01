package shell

import (
	"bytes"
	"strings"
	"testing"
)

func TestRaftLeaderTransfer_Name(t *testing.T) {
	cmd := &commandRaftLeaderTransfer{}
	expected := "cluster.raft.transferLeader"
	if cmd.Name() != expected {
		t.Errorf("expected name %q, got %q", expected, cmd.Name())
	}
}

func TestRaftLeaderTransfer_Help(t *testing.T) {
	cmd := &commandRaftLeaderTransfer{}
	help := cmd.Help()

	// Verify help text contains key information
	expectedPhrases := []string{
		"transfer raft leadership",
		"cluster.raft.transferLeader",
		"-id",
		"-address",
		"cluster.raft.ps",
		"-raftHashicorp",
	}

	for _, phrase := range expectedPhrases {
		if !strings.Contains(help, phrase) {
			t.Errorf("help text should contain %q", phrase)
		}
	}
}

func TestRaftLeaderTransfer_HasTag(t *testing.T) {
	cmd := &commandRaftLeaderTransfer{}
	// The command should not have any special tags
	if cmd.HasTag(ResourceHeavy) {
		t.Error("expected HasTag to return false for ResourceHeavy")
	}
}

func TestRaftLeaderTransfer_ValidateTargetIdWithoutAddress(t *testing.T) {
	cmd := &commandRaftLeaderTransfer{}
	var buf bytes.Buffer

	// Create a mock command environment - this will fail because no master client
	// but we can verify argument parsing
	err := cmd.Do([]string{"-id", "test-server"}, nil, &buf)

	// Should fail because -address is required when -id is specified
	if err == nil {
		t.Error("expected error when -id is specified without -address")
	}
	if err != nil && !strings.Contains(err.Error(), "-address is required") {
		t.Errorf("expected error about missing -address, got: %v", err)
	}
}

func TestRaftLeaderTransfer_ValidateTargetAddressWithoutId(t *testing.T) {
	cmd := &commandRaftLeaderTransfer{}
	var buf bytes.Buffer

	// Verify argument parsing - address without id should fail
	err := cmd.Do([]string{"-address", "localhost:19333"}, nil, &buf)

	// Should fail because -id is required when -address is specified
	if err == nil {
		t.Error("expected error when -address is specified without -id")
	}
	if err != nil && !strings.Contains(err.Error(), "-id is required") {
		t.Errorf("expected error about missing -id, got: %v", err)
	}
}

func TestRaftLeaderTransfer_UnknownFlag(t *testing.T) {
	cmd := &commandRaftLeaderTransfer{}
	var buf bytes.Buffer

	// Unknown flag should return an error
	err := cmd.Do([]string{"-unknown-flag"}, nil, &buf)
	if err == nil {
		t.Error("expected error for unknown flag")
	}
}
