package s3tables

import (
	"encoding/json"
	"strings"
	"testing"
)

func settingPtr(v int64) *int64 { return &v }

func TestValidateMaintenanceValueAcceptsScopedTypes(t *testing.T) {
	err := validateMaintenanceValue(MaintenanceTypeIcebergCompaction, tableMaintenanceTypes, &MaintenanceConfigurationValue{
		Status:   MaintenanceStatusEnabled,
		Settings: &MaintenanceSettings{IcebergCompaction: &IcebergCompactionSettings{TargetFileSizeMB: settingPtr(512)}},
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
			Settings: &MaintenanceSettings{IcebergSnapshotManagement: &IcebergSnapshotManagementSettings{MinSnapshotsToKeep: settingPtr(3)}},
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
			Settings: &MaintenanceSettings{IcebergCompaction: &IcebergCompactionSettings{TargetFileSizeMB: settingPtr(512)}},
		},
		MaintenanceTypeIcebergSnapshotManagement: {
			Status: MaintenanceStatusDisabled,
			Settings: &MaintenanceSettings{IcebergSnapshotManagement: &IcebergSnapshotManagementSettings{
				MinSnapshotsToKeep:  settingPtr(3),
				MaxSnapshotAgeHours: settingPtr(120),
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
	if got := compaction.Settings.IcebergCompaction.TargetFileSizeMB; got == nil || *got != 512 {
		t.Errorf("expected targetFileSizeMB=512, got %v", got)
	}

	snapshots := decoded[MaintenanceTypeIcebergSnapshotManagement]
	if snapshots == nil || snapshots.Status != MaintenanceStatusDisabled {
		t.Fatalf("expected snapshot management disabled, got %s", data)
	}
	if got := snapshots.Settings.IcebergSnapshotManagement.MaxSnapshotAgeHours; got == nil || *got != 120 {
		t.Errorf("expected maxSnapshotAgeHours=120, got %v", got)
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
			Settings: &MaintenanceSettings{IcebergCompaction: &IcebergCompactionSettings{Strategy: CompactionStrategySort, TargetFileSizeMB: settingPtr(512)}},
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
// attribute this package stores — including the ones absent right now, which a
// concurrent writer can create and this write would then delete.
func TestSnapshotExtendedCoversEveryAttribute(t *testing.T) {
	extended := map[string][]byte{
		ExtendedKeyMetadata:    []byte(`{"a":1}`),
		ExtendedKeyMaintenance: []byte(`{"icebergCompaction":{"status":"disabled"}}`),
	}

	expected := SnapshotExtended(extended, ExtendedKeyMaintenanceStatus)

	for key, want := range extended {
		if string(expected[key]) != string(want) {
			t.Errorf("attribute %s not carried into the precondition", key)
		}
	}
	for _, key := range s3tablesExtendedKeys {
		if _, ok := expected[key]; !ok {
			t.Errorf("attribute %s missing from the precondition", key)
		}
	}

	// The absent ones are asserted absent, so a concurrent create fails the write.
	for _, key := range []string{ExtendedKeyMaintenanceStatus, ExtendedKeyPolicy, ExtendedKeyTags} {
		if got, ok := expected[key]; !ok || len(got) != 0 {
			t.Errorf("expected %s asserted absent, got %v", key, got)
		}
	}

	// Mutating the snapshot must not disturb the entry it came from.
	expected[ExtendedKeyMetadata] = []byte("changed")
	if string(extended[ExtendedKeyMetadata]) != `{"a":1}` {
		t.Error("expected the source map left alone")
	}
}

// The regression this closes: a maintenance configuration that did not exist
// when the writer read the entry must still be asserted, or a first-time
// disable lands between the read and the write and gets erased.
func TestSnapshotExtendedAssertsAbsentMaintenanceKey(t *testing.T) {
	expected := SnapshotExtended(map[string][]byte{ExtendedKeyMetadata: []byte(`{}`)})

	value, ok := expected[ExtendedKeyMaintenance]
	if !ok {
		t.Fatal("expected the maintenance key asserted even when absent")
	}
	if len(value) != 0 {
		t.Errorf("expected an absent assertion, got %v", value)
	}
}

// AWS bounds every numeric setting to 1..2147483647. Accepting anything else
// would store a value the worker ignores or saturates, so the configuration
// read back would not be the one that runs.
func TestValidateMaintenanceSettingRange(t *testing.T) {
	compaction := func(v *int64) error {
		return validateMaintenanceValue(MaintenanceTypeIcebergCompaction, tableMaintenanceTypes, &MaintenanceConfigurationValue{
			Status:   MaintenanceStatusEnabled,
			Settings: &MaintenanceSettings{IcebergCompaction: &IcebergCompactionSettings{TargetFileSizeMB: v}},
		})
	}

	if err := compaction(nil); err != nil {
		t.Errorf("expected an unset setting accepted, got %v", err)
	}
	for _, v := range []int64{1, 512, maintenanceSettingMax} {
		if err := compaction(settingPtr(v)); err != nil {
			t.Errorf("expected %d accepted, got %v", v, err)
		}
	}
	for _, v := range []int64{0, -1, maintenanceSettingMax + 1, 1 << 62} {
		if err := compaction(settingPtr(v)); err == nil {
			t.Errorf("expected %d rejected", v)
		}
	}
}

func TestValidateMaintenanceSettingRangeCoversEveryField(t *testing.T) {
	cases := map[string]*MaintenanceSettings{
		"minSnapshotsToKeep":  {IcebergSnapshotManagement: &IcebergSnapshotManagementSettings{MinSnapshotsToKeep: settingPtr(0)}},
		"maxSnapshotAgeHours": {IcebergSnapshotManagement: &IcebergSnapshotManagementSettings{MaxSnapshotAgeHours: settingPtr(-5)}},
		"unreferencedDays":    {IcebergUnreferencedFileRemoval: &IcebergUnreferencedFileRemovalSettings{UnreferencedDays: settingPtr(maintenanceSettingMax + 1)}},
		"nonCurrentDays":      {IcebergUnreferencedFileRemoval: &IcebergUnreferencedFileRemovalSettings{NonCurrentDays: settingPtr(0)}},
	}

	for name, settings := range cases {
		t.Run(name, func(t *testing.T) {
			if err := validateMaintenanceSettings(settings); err == nil {
				t.Errorf("expected %s out of range to be rejected", name)
			}
		})
	}
}

// An explicit zero has to be distinguishable from an omitted field, or it is
// silently accepted and then ignored.
func TestMaintenanceSettingsDistinguishZeroFromUnset(t *testing.T) {
	var decoded MaintenanceSettings
	if err := json.Unmarshal([]byte(`{"icebergCompaction":{"targetFileSizeMB":0}}`), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.IcebergCompaction.TargetFileSizeMB == nil {
		t.Fatal("expected an explicit zero to survive decoding")
	}

	var omitted MaintenanceSettings
	if err := json.Unmarshal([]byte(`{"icebergCompaction":{}}`), &omitted); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if omitted.IcebergCompaction.TargetFileSizeMB != nil {
		t.Error("expected an omitted field to stay unset")
	}
}

// aws-cn and aws-us-gov ARNs are valid and must parse; the emitted ARN has to
// use the partition its region belongs to so it round-trips.
func TestARNPartitionsBeyondCommercial(t *testing.T) {
	for _, arn := range []string{
		"arn:aws:s3tables:us-east-1:123456789012:bucket/analytics",
		"arn:aws-cn:s3tables:cn-north-1:123456789012:bucket/analytics",
		"arn:aws-us-gov:s3tables:us-gov-west-1:123456789012:bucket/analytics",
	} {
		got, err := parseBucketNameFromARN(arn)
		if err != nil {
			t.Errorf("expected %s to parse, got %v", arn, err)
			continue
		}
		if got != "analytics" {
			t.Errorf("expected bucket analytics from %s, got %q", arn, got)
		}
	}

	if _, err := parseBucketNameFromARN("arn:notaws:s3tables:us-east-1:123456789012:bucket/analytics"); err == nil {
		t.Error("expected a non-AWS partition to be rejected")
	}
}

func TestARNPartitionForRegion(t *testing.T) {
	for region, want := range map[string]string{
		"us-east-1":       "aws",
		"eu-west-2":       "aws",
		"cn-north-1":      "aws-cn",
		"cn-northwest-1":  "aws-cn",
		"us-gov-west-1":   "aws-us-gov",
		"us-iso-east-1":   "aws-iso",
		"us-isob-east-1":  "aws-iso-b",
		"eu-isoe-west-1":  "aws-iso-e",
		"us-isof-south-1": "aws-iso-f",
		"eusc-de-east-1":  "aws-eusc",
	} {
		if got := arnPartitionForRegion(region); got != want {
			t.Errorf("region %s: expected partition %q, got %q", region, want, got)
		}
	}
}

// An ARN this handler emits has to carry the partition its region belongs to,
// not just parse back — a commercial ARN returned for a gov region still parses
// but is wrong for IAM matching and for the caller.
func TestGeneratedARNsUseTheRegionPartition(t *testing.T) {
	for region, partition := range map[string]string{
		"us-east-1":     "aws",
		"cn-north-1":    "aws-cn",
		"us-gov-west-1": "aws-us-gov",
	} {
		h := NewS3TablesHandler()
		h.SetRegion(region)

		bucketARN := h.generateTableBucketARN(DefaultAccountID, "analytics")
		tableARN := h.generateTableARN(DefaultAccountID, "analytics", "sales/orders")
		viewARN := h.generateViewARN(DefaultAccountID, "analytics", "sales/v1")
		s3ARN := h.generateS3BucketARN("analytics")

		for _, arn := range []string{bucketARN, tableARN, viewARN, s3ARN} {
			if !strings.HasPrefix(arn, "arn:"+partition+":") {
				t.Errorf("region %s: expected partition %q, got %s", region, partition, arn)
			}
		}

		got, err := parseBucketNameFromARN(bucketARN)
		if err != nil {
			t.Errorf("generated ARN %s does not parse: %v", bucketARN, err)
			continue
		}
		if got != "analytics" {
			t.Errorf("expected analytics from %s, got %q", bucketARN, got)
		}

		bucket, namespace, table, err := parseTableFromARN(tableARN)
		if err != nil {
			t.Errorf("generated table ARN %s does not parse: %v", tableARN, err)
			continue
		}
		if bucket != "analytics" || namespace != "sales" || table != "orders" {
			t.Errorf("unexpected parse of %s: %s/%s/%s", tableARN, bucket, namespace, table)
		}
	}
}
