package command

import "testing"

func TestMiniDefaultPluginJobTypes(t *testing.T) {
	jobTypes, err := parsePluginWorkerJobTypes(defaultMiniPluginJobTypes)
	if err != nil {
		t.Fatalf("parsePluginWorkerJobTypes(mini default) err = %v", err)
	}
	if len(jobTypes) != 3 {
		t.Fatalf("expected mini default job types to include 3 handlers, got %v", jobTypes)
	}
}
