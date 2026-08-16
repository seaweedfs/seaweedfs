package iceberg

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func baseTestConfig() Config {
	return applyThresholdDefaults(Config{
		SnapshotRetentionMs:     hoursToMs(defaultSnapshotRetentionHours),
		MaxSnapshotsToKeep:      defaultMaxSnapshotsToKeep,
		TablePropertiesOverride: true,
	})
}

func TestResolveTableConfigNoProperties(t *testing.T) {
	base := baseTestConfig()

	got := resolveTableConfig(base, iceberg.Properties{}, nil)
	if got != base {
		t.Errorf("expected config untouched, got %+v", got)
	}

	if got := resolveTableConfig(base, nil, nil); got != base {
		t.Errorf("expected config untouched for nil properties, got %+v", got)
	}
}

func TestResolveTableConfigPropertiesWin(t *testing.T) {
	base := baseTestConfig()
	got := resolveTableConfig(base, iceberg.Properties{
		propTargetFileSize:       "536870912",
		propDeleteTargetFileSize: "33554432",
		propMaxSnapshotAgeMs:     "432000000",
		propMinSnapshotsToKeep:   "1",
	}, nil)

	if got.TargetFileSizeBytes != 536870912 {
		t.Errorf("expected TargetFileSizeBytes=536870912, got %d", got.TargetFileSizeBytes)
	}
	if got.DeleteTargetFileSizeBytes != 33554432 {
		t.Errorf("expected DeleteTargetFileSizeBytes=33554432, got %d", got.DeleteTargetFileSizeBytes)
	}
	if got.SnapshotRetentionMs != 432000000 {
		t.Errorf("expected SnapshotRetentionMs=432000000, got %d", got.SnapshotRetentionMs)
	}
	if got.MaxSnapshotsToKeep != 1 {
		t.Errorf("expected MaxSnapshotsToKeep=1, got %d", got.MaxSnapshotsToKeep)
	}
}

// A sub-hour retention has to survive resolution; truncating it to whole hours
// would round down to zero and get clamped back up to the default.
func TestResolveTableConfigSubHourRetention(t *testing.T) {
	got := resolveTableConfig(baseTestConfig(), iceberg.Properties{propMaxSnapshotAgeMs: "1"}, nil)
	if got.SnapshotRetentionMs != 1 {
		t.Errorf("expected SnapshotRetentionMs=1, got %d", got.SnapshotRetentionMs)
	}
}

func TestResolveTableConfigIgnoresUnusableValues(t *testing.T) {
	base := baseTestConfig()

	for name, value := range map[string]string{
		"not a number": "512mb",
		"empty":        "",
		"zero":         "0",
		"negative":     "-1",
		"overflow":     "99999999999999999999",
	} {
		t.Run(name, func(t *testing.T) {
			got := resolveTableConfig(base, iceberg.Properties{propTargetFileSize: value}, nil)
			if got.TargetFileSizeBytes != base.TargetFileSizeBytes {
				t.Errorf("expected fallback to %d, got %d", base.TargetFileSizeBytes, got.TargetFileSizeBytes)
			}
		})
	}
}

func TestResolveTableConfigTrimsWhitespace(t *testing.T) {
	got := resolveTableConfig(baseTestConfig(), iceberg.Properties{propTargetFileSize: "  536870912\n"}, nil)
	if got.TargetFileSizeBytes != 536870912 {
		t.Errorf("expected TargetFileSizeBytes=536870912, got %d", got.TargetFileSizeBytes)
	}
}

func TestResolveTableConfigLeavesOtherFields(t *testing.T) {
	base := baseTestConfig()
	base.Operations = "compact"
	base.Where = "day = 3"
	base.MinInputFiles = 9

	got := resolveTableConfig(base, iceberg.Properties{propTargetFileSize: "536870912"}, nil)
	if got.Operations != "compact" || got.Where != "day = 3" || got.MinInputFiles != 9 {
		t.Errorf("expected unrelated fields preserved, got %+v", got)
	}
}

// An orphan cutoff large enough to overflow time.Duration would wrap negative,
// put the cutoff in the future and make every file look like an orphan.
func TestApplyThresholdDefaultsClampsOrphanCutoff(t *testing.T) {
	got := applyThresholdDefaults(Config{OrphanOlderThanHours: math.MaxInt64})
	if got.OrphanOlderThanHours != maxOrphanOlderThanHours {
		t.Fatalf("expected the cutoff clamped to %d, got %d", maxOrphanOlderThanHours, got.OrphanOlderThanHours)
	}
	if cutoff := time.Duration(got.OrphanOlderThanHours) * time.Hour; cutoff <= 0 {
		t.Errorf("expected a positive cutoff duration, got %v", cutoff)
	}

	if got := applyThresholdDefaults(Config{OrphanOlderThanHours: 72}); got.OrphanOlderThanHours != 72 {
		t.Errorf("expected a normal cutoff untouched, got %d", got.OrphanOlderThanHours)
	}
}

func maintenanceConfig(entries map[string]*s3tables.MaintenanceConfigurationValue) s3tables.MaintenanceConfiguration {
	return s3tables.MaintenanceConfiguration(entries)
}

func TestResolveTableConfigAppliesMaintenanceConfig(t *testing.T) {
	got := resolveTableConfig(baseTestConfig(), nil, maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
		s3tables.MaintenanceTypeIcebergCompaction: {
			Status:   s3tables.MaintenanceStatusEnabled,
			Settings: &s3tables.MaintenanceSettings{IcebergCompaction: &s3tables.IcebergCompactionSettings{TargetFileSizeMB: 512}},
		},
		s3tables.MaintenanceTypeIcebergSnapshotManagement: {
			Status: s3tables.MaintenanceStatusEnabled,
			Settings: &s3tables.MaintenanceSettings{IcebergSnapshotManagement: &s3tables.IcebergSnapshotManagementSettings{
				MinSnapshotsToKeep:  2,
				MaxSnapshotAgeHours: 120,
			}},
		},
	}))

	if got.TargetFileSizeBytes != 512*bytesPerMB {
		t.Errorf("expected TargetFileSizeBytes=%d, got %d", 512*bytesPerMB, got.TargetFileSizeBytes)
	}
	if got.MaxSnapshotsToKeep != 2 {
		t.Errorf("expected MaxSnapshotsToKeep=2, got %d", got.MaxSnapshotsToKeep)
	}
	if got.SnapshotRetentionMs != hoursToMs(120) {
		t.Errorf("expected SnapshotRetentionMs=%d, got %d", hoursToMs(120), got.SnapshotRetentionMs)
	}
}

// The default: a table declaring its own layout beats the control plane, so a
// writer and the compactor cannot disagree about target file size.
func TestResolveTableConfigPropertyBeatsMaintenanceConfig(t *testing.T) {
	maintenance := maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
		s3tables.MaintenanceTypeIcebergCompaction: {
			Status:   s3tables.MaintenanceStatusEnabled,
			Settings: &s3tables.MaintenanceSettings{IcebergCompaction: &s3tables.IcebergCompactionSettings{TargetFileSizeMB: 128}},
		},
	})
	props := iceberg.Properties{propTargetFileSize: "536870912"}

	got := resolveTableConfig(baseTestConfig(), props, maintenance)
	if got.TargetFileSizeBytes != 536870912 {
		t.Errorf("expected the property to win with 536870912, got %d", got.TargetFileSizeBytes)
	}

	base := baseTestConfig()
	base.TablePropertiesOverride = false
	got = resolveTableConfig(base, props, maintenance)
	if got.TargetFileSizeBytes != 128*bytesPerMB {
		t.Errorf("expected the maintenance config to win with %d, got %d", 128*bytesPerMB, got.TargetFileSizeBytes)
	}
}

// A disabled type drops its operations rather than contributing settings.
func TestResolveTableConfigIgnoresDisabledSettings(t *testing.T) {
	base := baseTestConfig()
	got := resolveTableConfig(base, nil, maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
		s3tables.MaintenanceTypeIcebergCompaction: {
			Status:   s3tables.MaintenanceStatusDisabled,
			Settings: &s3tables.MaintenanceSettings{IcebergCompaction: &s3tables.IcebergCompactionSettings{TargetFileSizeMB: 512}},
		},
	}))

	if got.TargetFileSizeBytes != base.TargetFileSizeBytes {
		t.Errorf("expected disabled settings ignored, got %d", got.TargetFileSizeBytes)
	}
}

func TestFilterDisabledOperations(t *testing.T) {
	all := []string{"compact", "rewrite_position_delete_files", "expire_snapshots", "remove_orphans", "rewrite_manifests"}

	if got := filterDisabledOperations(all, nil); len(got) != len(all) {
		t.Errorf("expected no filtering without a configuration, got %v", got)
	}

	got := filterDisabledOperations(all, maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
		s3tables.MaintenanceTypeIcebergCompaction:              {Status: s3tables.MaintenanceStatusDisabled},
		s3tables.MaintenanceTypeIcebergUnreferencedFileRemoval: {Status: s3tables.MaintenanceStatusDisabled},
	}))
	want := []string{"expire_snapshots"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Errorf("expected %v, got %v", want, got)
	}

	if got := filterDisabledOperations(all, maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
		s3tables.MaintenanceTypeIcebergCompaction: {Status: s3tables.MaintenanceStatusEnabled},
	})); len(got) != len(all) {
		t.Errorf("expected an enabled type to filter nothing, got %v", got)
	}
}

func TestMergeMaintenanceConfigurationTableWins(t *testing.T) {
	bucket := maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
		s3tables.MaintenanceTypeIcebergUnreferencedFileRemoval: {Status: s3tables.MaintenanceStatusEnabled},
		s3tables.MaintenanceTypeIcebergCompaction:              {Status: s3tables.MaintenanceStatusEnabled},
	})
	table := maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
		s3tables.MaintenanceTypeIcebergCompaction: {Status: s3tables.MaintenanceStatusDisabled},
	})

	merged := mergeMaintenanceConfiguration(bucket, table)
	if merged[s3tables.MaintenanceTypeIcebergCompaction].Status != s3tables.MaintenanceStatusDisabled {
		t.Error("expected the table entry to win")
	}
	if merged[s3tables.MaintenanceTypeIcebergUnreferencedFileRemoval].Status != s3tables.MaintenanceStatusEnabled {
		t.Error("expected the bucket-only entry retained")
	}

	if got := mergeMaintenanceConfiguration(nil, table); len(got) != 1 {
		t.Errorf("expected the table configuration passed through, got %v", got)
	}
	if got := mergeMaintenanceConfiguration(bucket, nil); len(got) != 2 {
		t.Errorf("expected the bucket configuration passed through, got %v", got)
	}
}

func TestParseMaintenanceConfiguration(t *testing.T) {
	got, err := parseMaintenanceConfiguration(nil, "bucket")
	if err != nil || got != nil {
		t.Errorf("expected nil for a missing attribute, got %v (%v)", got, err)
	}

	// Unreadable must be an error, not an empty configuration: empty would run
	// operations the operator disabled.
	if _, err := parseMaintenanceConfiguration(map[string][]byte{s3tables.ExtendedKeyMaintenance: []byte("{")}, "bucket"); err == nil {
		t.Error("expected an error for a malformed attribute")
	}

	data := []byte(`{"icebergCompaction":{"status":"disabled"}}`)
	got, err = parseMaintenanceConfiguration(map[string][]byte{s3tables.ExtendedKeyMaintenance: data}, "bucket")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got[s3tables.MaintenanceTypeIcebergCompaction].Status != s3tables.MaintenanceStatusDisabled {
		t.Errorf("expected compaction disabled, got %v", got)
	}
}

// unreferencedDays large enough to overflow must not land a cutoff in the future.
func TestResolveTableConfigClampsUnreferencedDays(t *testing.T) {
	got := resolveTableConfig(baseTestConfig(), nil, maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
		s3tables.MaintenanceTypeIcebergUnreferencedFileRemoval: {
			Status: s3tables.MaintenanceStatusEnabled,
			Settings: &s3tables.MaintenanceSettings{
				IcebergUnreferencedFileRemoval: &s3tables.IcebergUnreferencedFileRemovalSettings{UnreferencedDays: math.MaxInt64},
			},
		},
	}))

	if got.OrphanOlderThanHours != maxOrphanOlderThanHours {
		t.Fatalf("expected the cutoff clamped to %d, got %d", maxOrphanOlderThanHours, got.OrphanOlderThanHours)
	}
	if cutoff := time.Duration(got.OrphanOlderThanHours) * time.Hour; cutoff <= 0 {
		t.Errorf("expected a positive cutoff duration, got %v", cutoff)
	}

	got = resolveTableConfig(baseTestConfig(), nil, maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
		s3tables.MaintenanceTypeIcebergUnreferencedFileRemoval: {
			Status: s3tables.MaintenanceStatusEnabled,
			Settings: &s3tables.MaintenanceSettings{
				IcebergUnreferencedFileRemoval: &s3tables.IcebergUnreferencedFileRemovalSettings{UnreferencedDays: 3},
			},
		},
	}))
	if got.OrphanOlderThanHours != 72 {
		t.Errorf("expected 3 days to be 72 hours, got %d", got.OrphanOlderThanHours)
	}
}

// AWS marks a file non-current after unreferencedDays and deletes it
// nonCurrentDays later; deleting in one step has to wait for both.
func TestResolveTableConfigAddsNonCurrentWindow(t *testing.T) {
	orphan := func(unreferenced, nonCurrent int64) Config {
		return resolveTableConfig(baseTestConfig(), nil, maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
			s3tables.MaintenanceTypeIcebergUnreferencedFileRemoval: {
				Status: s3tables.MaintenanceStatusEnabled,
				Settings: &s3tables.MaintenanceSettings{
					IcebergUnreferencedFileRemoval: &s3tables.IcebergUnreferencedFileRemovalSettings{
						UnreferencedDays: unreferenced,
						NonCurrentDays:   nonCurrent,
					},
				},
			},
		}))
	}

	if got := orphan(3, 10); got.OrphanOlderThanHours != 13*24 {
		t.Errorf("expected 3+10 days to be %d hours, got %d", 13*24, got.OrphanOlderThanHours)
	}
	if got := orphan(0, 10); got.OrphanOlderThanHours != 10*24 {
		t.Errorf("expected nonCurrentDays alone to apply, got %d", got.OrphanOlderThanHours)
	}
	if got := orphan(math.MaxInt64, math.MaxInt64); got.OrphanOlderThanHours != maxOrphanOlderThanHours {
		t.Errorf("expected the sum clamped, got %d", got.OrphanOlderThanHours)
	}
}

func TestBuildJobStatus(t *testing.T) {
	ranAt := time.Unix(1755250000, 0).UTC()

	jobStatus := buildJobStatus(map[string]error{
		"compact":          nil,
		"expire_snapshots": nil,
	}, ranAt)

	compaction := jobStatus[s3tables.MaintenanceTypeIcebergCompaction]
	if compaction == nil || compaction.Status != s3tables.MaintenanceJobStatusSuccessful {
		t.Fatalf("expected compaction successful, got %+v", compaction)
	}
	if compaction.LastRunTimestamp == nil || !compaction.LastRunTimestamp.Equal(ranAt) {
		t.Errorf("expected lastRunTimestamp %v, got %v", ranAt, compaction.LastRunTimestamp)
	}
	if _, ok := jobStatus[s3tables.MaintenanceTypeIcebergUnreferencedFileRemoval]; ok {
		t.Error("expected an operation that did not run to be left out")
	}
}

// Several operations share one AWS job type, so one failure fails the type.
func TestBuildJobStatusFailureWins(t *testing.T) {
	jobStatus := buildJobStatus(map[string]error{
		"compact":           nil,
		"rewrite_manifests": errors.New("commit conflict"),
	}, time.Unix(1755250000, 0).UTC())

	compaction := jobStatus[s3tables.MaintenanceTypeIcebergCompaction]
	if compaction.Status != s3tables.MaintenanceJobStatusFailed {
		t.Errorf("expected compaction failed, got %q", compaction.Status)
	}
	if compaction.FailureMessage != "commit conflict" {
		t.Errorf("expected the failure message carried, got %q", compaction.FailureMessage)
	}
}

func TestBuildJobStatusEmpty(t *testing.T) {
	if got := buildJobStatus(nil, time.Now()); len(got) != 0 {
		t.Errorf("expected an empty status, got %v", got)
	}
}

func TestResolveTableConfigAppliesCompactionStrategy(t *testing.T) {
	withStrategy := func(strategy string) Config {
		return resolveTableConfig(baseTestConfig(), nil, maintenanceConfig(map[string]*s3tables.MaintenanceConfigurationValue{
			s3tables.MaintenanceTypeIcebergCompaction: {
				Status:   s3tables.MaintenanceStatusEnabled,
				Settings: &s3tables.MaintenanceSettings{IcebergCompaction: &s3tables.IcebergCompactionSettings{Strategy: strategy}},
			},
		}))
	}

	if got := withStrategy(s3tables.CompactionStrategySort); got.RewriteStrategy != "sort" {
		t.Errorf("expected sort, got %q", got.RewriteStrategy)
	}
	if got := withStrategy(s3tables.CompactionStrategyBinpack); got.RewriteStrategy != "binpack" {
		t.Errorf("expected binpack, got %q", got.RewriteStrategy)
	}

	// "auto" is carried through so the plan can pick per table.
	if got := withStrategy(s3tables.CompactionStrategyAuto); got.RewriteStrategy != rewriteStrategyAuto {
		t.Errorf("expected auto preserved, got %q", got.RewriteStrategy)
	}
}

// AWS defines auto as sorting tables that declare a sort order and bin-packing
// the rest, so it must not collapse to the worker default.
func TestResolveCompactionRewritePlanAuto(t *testing.T) {
	cfg := baseTestConfig()
	cfg.RewriteStrategy = rewriteStrategyAuto

	unsorted := buildTestMetadata(t, nil)
	plan, err := resolveCompactionRewritePlan(cfg, unsorted)
	if err != nil {
		t.Fatalf("auto must not fail on an unsorted table: %v", err)
	}
	if plan.strategy != defaultRewriteStrategy {
		t.Errorf("expected binpack for an unsorted table, got %q", plan.strategy)
	}

	// An explicit sort request on the same table is still an error, so auto is
	// doing something the existing strategies did not.
	cfg.RewriteStrategy = "sort"
	if _, err := resolveCompactionRewritePlan(cfg, unsorted); err == nil {
		t.Error("expected explicit sort to fail without a table sort order")
	}
}
