package command

import "testing"

func TestWorkerDefaultJobTypes(t *testing.T) {
	jobTypes, err := parsePluginWorkerJobTypes(*workerJobType)
	if err != nil {
		t.Fatalf("parsePluginWorkerJobTypes(default worker flag) err = %v", err)
	}
	if len(jobTypes) != 4 {
		t.Fatalf("expected default worker job types to include 4 handlers, got %v", jobTypes)
	}
}
