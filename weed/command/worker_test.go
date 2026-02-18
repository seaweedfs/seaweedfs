package command

import "testing"

func TestCanonicalizeLegacyWorkerCapabilities(t *testing.T) {
	jobTypes, err := canonicalizeLegacyWorkerCapabilities("vacuum,ec,balance,vacuum")
	if err != nil {
		t.Fatalf("canonicalizeLegacyWorkerCapabilities err = %v", err)
	}
	if jobTypes != "vacuum,erasure_coding,volume_balance" {
		t.Fatalf("unexpected mapped job types: %q", jobTypes)
	}
}

func TestCanonicalizeLegacyWorkerCapabilitiesRejectsRemovedTypes(t *testing.T) {
	if _, err := canonicalizeLegacyWorkerCapabilities("replication"); err == nil {
		t.Fatalf("expected unsupported legacy capability error")
	}
	if _, err := canonicalizeLegacyWorkerCapabilities("remote"); err == nil {
		t.Fatalf("expected unsupported legacy capability error")
	}
}

func TestWorkerDefaultJobTypes(t *testing.T) {
	jobTypes, err := parsePluginWorkerJobTypes(*workerJobType)
	if err != nil {
		t.Fatalf("parsePluginWorkerJobTypes(default worker flag) err = %v", err)
	}
	if len(jobTypes) != 3 {
		t.Fatalf("expected default worker job types to include 3 handlers, got %v", jobTypes)
	}
}
