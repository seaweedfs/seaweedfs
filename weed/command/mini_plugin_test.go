package command

import "testing"

func TestMiniDefaultPluginJobTypesIncludeDummy(t *testing.T) {
	jobTypes, err := parsePluginWorkerJobTypes(defaultMiniPluginJobTypes)
	if err != nil {
		t.Fatalf("parsePluginWorkerJobTypes(mini default) err = %v", err)
	}

	foundDummy := false
	for _, jobType := range jobTypes {
		if jobType == "dummy_stress" {
			foundDummy = true
			break
		}
	}
	if !foundDummy {
		t.Fatalf("expected mini default job types to include dummy_stress, got %v", jobTypes)
	}
}
