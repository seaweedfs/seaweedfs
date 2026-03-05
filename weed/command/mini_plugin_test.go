package command

import "testing"

func TestMiniDefaultPluginJobTypes(t *testing.T) {
	jobTypes, err := parsePluginWorkerJobTypes(defaultMiniPluginJobTypes)
	if err != nil {
		t.Fatalf("parsePluginWorkerJobTypes(mini default) err = %v", err)
	}
	if len(jobTypes) != 4 {
		t.Fatalf("expected mini default job types to include 4 handlers, got %v", jobTypes)
	}
}
