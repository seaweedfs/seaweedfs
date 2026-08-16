package s3tables

import (
	"encoding/json"
	"testing"
)

func TestValidateMaintenanceValueAcceptsScopedTypes(t *testing.T) {
	err := validateMaintenanceValue(MaintenanceTypeIcebergCompaction, tableMaintenanceTypes, &MaintenanceConfigurationValue{
		Status:   MaintenanceStatusEnabled,
		Settings: &MaintenanceSettings{IcebergCompaction: &IcebergCompactionSettings{TargetFileSizeMB: 512}},
	})
	if err != nil {
		t.Errorf("expected compaction accepted on a table, got %v", err)
	}

	err = validateMaintenanceValue(MaintenanceTypeIcebergUnreferencedFileRemoval, bucketMaintenanceTypes, &MaintenanceConfigurationValue{
		Status: MaintenanceStatusDisabled,
	})
	if err != nil {
		t.Errorf("expected unreferenced file removal accepted on a bucket, got %v", err)
	}
}

func TestValidateMaintenanceValueRejectsWrongScope(t *testing.T) {
	if err := validateMaintenanceValue(MaintenanceTypeIcebergCompaction, bucketMaintenanceTypes, &MaintenanceConfigurationValue{
		Status: MaintenanceStatusEnabled,
	}); err == nil {
		t.Error("expected compaction rejected on a table bucket")
	}

	if err := validateMaintenanceValue(MaintenanceTypeIcebergUnreferencedFileRemoval, tableMaintenanceTypes, &MaintenanceConfigurationValue{
		Status: MaintenanceStatusEnabled,
	}); err == nil {
		t.Error("expected unreferenced file removal rejected on a table")
	}
}

func TestValidateMaintenanceValueRejectsBadInput(t *testing.T) {
	cases := map[string]struct {
		maintenanceType string
		value           *MaintenanceConfigurationValue
	}{
		"empty type":   {"", &MaintenanceConfigurationValue{Status: MaintenanceStatusEnabled}},
		"unknown type": {"icebergSomethingElse", &MaintenanceConfigurationValue{Status: MaintenanceStatusEnabled}},
		"nil value":    {MaintenanceTypeIcebergCompaction, nil},
		"no status":    {MaintenanceTypeIcebergCompaction, &MaintenanceConfigurationValue{}},
		"bad status":   {MaintenanceTypeIcebergCompaction, &MaintenanceConfigurationValue{Status: "Enabled"}},
		"settings for another type": {MaintenanceTypeIcebergCompaction, &MaintenanceConfigurationValue{
			Status:   MaintenanceStatusEnabled,
			Settings: &MaintenanceSettings{IcebergSnapshotManagement: &IcebergSnapshotManagementSettings{MinSnapshotsToKeep: 3}},
		}},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if err := validateMaintenanceValue(tc.maintenanceType, tableMaintenanceTypes, tc.value); err == nil {
				t.Error("expected rejection")
			}
		})
	}
}

// The stored attribute is the wire shape, so Get can hand it straight back.
func TestMaintenanceConfigurationRoundTrip(t *testing.T) {
	original := MaintenanceConfiguration{
		MaintenanceTypeIcebergCompaction: {
			Status:   MaintenanceStatusEnabled,
			Settings: &MaintenanceSettings{IcebergCompaction: &IcebergCompactionSettings{TargetFileSizeMB: 512}},
		},
		MaintenanceTypeIcebergSnapshotManagement: {
			Status: MaintenanceStatusDisabled,
			Settings: &MaintenanceSettings{IcebergSnapshotManagement: &IcebergSnapshotManagementSettings{
				MinSnapshotsToKeep:  3,
				MaxSnapshotAgeHours: 120,
			}},
		},
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded MaintenanceConfiguration
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	compaction := decoded[MaintenanceTypeIcebergCompaction]
	if compaction == nil || compaction.Settings == nil || compaction.Settings.IcebergCompaction == nil {
		t.Fatalf("expected compaction settings, got %s", data)
	}
	if compaction.Settings.IcebergCompaction.TargetFileSizeMB != 512 {
		t.Errorf("expected targetFileSizeMB=512, got %d", compaction.Settings.IcebergCompaction.TargetFileSizeMB)
	}

	snapshots := decoded[MaintenanceTypeIcebergSnapshotManagement]
	if snapshots == nil || snapshots.Status != MaintenanceStatusDisabled {
		t.Fatalf("expected snapshot management disabled, got %s", data)
	}
	if snapshots.Settings.IcebergSnapshotManagement.MaxSnapshotAgeHours != 120 {
		t.Errorf("expected maxSnapshotAgeHours=120, got %d", snapshots.Settings.IcebergSnapshotManagement.MaxSnapshotAgeHours)
	}
}

// Putting one type must not drop the others already stored.
func TestMaintenanceConfigurationMergeKeepsOtherTypes(t *testing.T) {
	stored, err := json.Marshal(MaintenanceConfiguration{
		MaintenanceTypeIcebergCompaction: {Status: MaintenanceStatusEnabled},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	config := MaintenanceConfiguration{}
	if err := json.Unmarshal(stored, &config); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	config[MaintenanceTypeIcebergSnapshotManagement] = &MaintenanceConfigurationValue{Status: MaintenanceStatusDisabled}

	if len(config) != 2 {
		t.Errorf("expected both types retained, got %+v", config)
	}
	if config[MaintenanceTypeIcebergCompaction].Status != MaintenanceStatusEnabled {
		t.Error("expected the existing compaction entry preserved")
	}
}

func TestParseTableTarget(t *testing.T) {
	h := NewS3TablesHandler()
	arn := h.generateTableBucketARN(DefaultAccountID, "analytics")

	bucketName, namespaceName, tableName, err := h.parseTableTarget(arn, []string{"sales"}, "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bucketName != "analytics" || namespaceName != "sales" || tableName != "orders" {
		t.Errorf("got %q/%q/%q", bucketName, namespaceName, tableName)
	}

	for name, tc := range map[string]struct {
		arn       string
		namespace []string
		table     string
	}{
		"missing arn":       {"", []string{"sales"}, "orders"},
		"missing namespace": {arn, nil, "orders"},
		"missing name":      {arn, []string{"sales"}, ""},
		"bad arn":           {"not-an-arn", []string{"sales"}, "orders"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := h.parseTableTarget(tc.arn, tc.namespace, tc.table); err == nil {
				t.Error("expected rejection")
			}
		})
	}
}

func TestBuildJobStatusResponseReportsEveryType(t *testing.T) {
	status := buildJobStatusResponse(nil, nil)
	if len(status) != 3 {
		t.Fatalf("expected all three job types reported, got %v", status)
	}
	for jobType, value := range status {
		if value.Status != MaintenanceJobStatusNotYetRun {
			t.Errorf("expected %s not yet run, got %q", jobType, value.Status)
		}
	}
}

func TestBuildJobStatusResponseDisabledBeatsRecorded(t *testing.T) {
	recorded := MaintenanceJobStatus{
		MaintenanceTypeIcebergCompaction:         {Status: MaintenanceJobStatusSuccessful},
		MaintenanceTypeIcebergSnapshotManagement: {Status: MaintenanceJobStatusFailed, FailureMessage: "boom"},
	}
	config := MaintenanceConfiguration{
		MaintenanceTypeIcebergCompaction: {Status: MaintenanceStatusDisabled},
	}

	status := buildJobStatusResponse(recorded, config)
	if status[MaintenanceTypeIcebergCompaction].Status != MaintenanceJobStatusDisabled {
		t.Errorf("expected a disabled type reported as disabled, got %q", status[MaintenanceTypeIcebergCompaction].Status)
	}
	if status[MaintenanceTypeIcebergSnapshotManagement].FailureMessage != "boom" {
		t.Error("expected the recorded failure preserved")
	}
	if status[MaintenanceTypeIcebergUnreferencedFileRemoval].Status != MaintenanceJobStatusNotYetRun {
		t.Error("expected an unrun type reported as not yet run")
	}
}

// A strategy the worker cannot carry out must be rejected, not accepted and
// silently compacted some other way.
func TestValidateMaintenanceValueCompactionStrategy(t *testing.T) {
	for _, strategy := range []string{"", CompactionStrategyAuto, CompactionStrategyBinpack, CompactionStrategySort} {
		if err := validateMaintenanceValue(MaintenanceTypeIcebergCompaction, tableMaintenanceTypes, &MaintenanceConfigurationValue{
			Status:   MaintenanceStatusEnabled,
			Settings: &MaintenanceSettings{IcebergCompaction: &IcebergCompactionSettings{Strategy: strategy}},
		}); err != nil {
			t.Errorf("expected strategy %q accepted, got %v", strategy, err)
		}
	}

	for _, strategy := range []string{CompactionStrategyZOrder, "nonsense"} {
		if err := validateMaintenanceValue(MaintenanceTypeIcebergCompaction, tableMaintenanceTypes, &MaintenanceConfigurationValue{
			Status:   MaintenanceStatusEnabled,
			Settings: &MaintenanceSettings{IcebergCompaction: &IcebergCompactionSettings{Strategy: strategy}},
		}); err == nil {
			t.Errorf("expected strategy %q rejected", strategy)
		}
	}
}

// Put then Get must return the strategy unchanged.
func TestMaintenanceConfigurationPreservesStrategy(t *testing.T) {
	data, err := json.Marshal(MaintenanceConfiguration{
		MaintenanceTypeIcebergCompaction: {
			Status:   MaintenanceStatusEnabled,
			Settings: &MaintenanceSettings{IcebergCompaction: &IcebergCompactionSettings{Strategy: CompactionStrategySort, TargetFileSizeMB: 512}},
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded MaintenanceConfiguration
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := decoded[MaintenanceTypeIcebergCompaction].Settings.IcebergCompaction.Strategy; got != CompactionStrategySort {
		t.Errorf("expected the strategy preserved, got %q", got)
	}
}

// Unreferenced file removal is configured on the bucket, so a bucket-level
// disable has to surface here rather than a stale table status.
func TestBuildJobStatusResponseHonoursBucketDisable(t *testing.T) {
	recorded := MaintenanceJobStatus{
		MaintenanceTypeIcebergUnreferencedFileRemoval: {Status: MaintenanceJobStatusSuccessful},
	}
	bucketConfig := MaintenanceConfiguration{
		MaintenanceTypeIcebergUnreferencedFileRemoval: {Status: MaintenanceStatusDisabled},
	}

	status := buildJobStatusResponse(recorded, MergeMaintenanceConfiguration(bucketConfig, nil))
	if got := status[MaintenanceTypeIcebergUnreferencedFileRemoval].Status; got != MaintenanceJobStatusDisabled {
		t.Errorf("expected Disabled from the bucket configuration, got %q", got)
	}
}

func TestMergeMaintenanceConfiguration(t *testing.T) {
	bucket := MaintenanceConfiguration{
		MaintenanceTypeIcebergUnreferencedFileRemoval: {Status: MaintenanceStatusEnabled},
		MaintenanceTypeIcebergCompaction:              {Status: MaintenanceStatusEnabled},
	}
	table := MaintenanceConfiguration{
		MaintenanceTypeIcebergCompaction: {Status: MaintenanceStatusDisabled},
	}

	merged := MergeMaintenanceConfiguration(bucket, table)
	if merged[MaintenanceTypeIcebergCompaction].Status != MaintenanceStatusDisabled {
		t.Error("expected the table entry to win")
	}
	if merged[MaintenanceTypeIcebergUnreferencedFileRemoval].Status != MaintenanceStatusEnabled {
		t.Error("expected the bucket-only entry retained")
	}
	if got := MergeMaintenanceConfiguration(nil, table); len(got) != 1 {
		t.Errorf("expected the table configuration passed through, got %v", got)
	}
	if got := MergeMaintenanceConfiguration(bucket, nil); len(got) != 2 {
		t.Errorf("expected the bucket configuration passed through, got %v", got)
	}
}

// UpdateEntry rewrites the whole entry, so the precondition has to cover every
// attribute or this write silently reverts a concurrent one.
func TestSnapshotExtendedCoversEveryAttribute(t *testing.T) {
	extended := map[string][]byte{
		ExtendedKeyMetadata:    []byte(`{"a":1}`),
		ExtendedKeyMaintenance: []byte(`{"icebergCompaction":{"status":"disabled"}}`),
	}

	expected := SnapshotExtended(extended, ExtendedKeyMaintenanceStatus)
	if len(expected) != 3 {
		t.Fatalf("expected every attribute plus the target key, got %v", expected)
	}
	for key, want := range extended {
		if string(expected[key]) != string(want) {
			t.Errorf("attribute %s not carried into the precondition", key)
		}
	}
	// The target key is asserted absent, so a concurrent create fails the write.
	if got, ok := expected[ExtendedKeyMaintenanceStatus]; !ok || len(got) != 0 {
		t.Errorf("expected the target key asserted absent, got %v", got)
	}

	// Mutating the snapshot must not disturb the entry it came from.
	expected[ExtendedKeyMetadata] = []byte("changed")
	if string(extended[ExtendedKeyMetadata]) != `{"a":1}` {
		t.Error("expected the source map left alone")
	}
}
