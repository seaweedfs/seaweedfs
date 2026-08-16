package iceberg

import (
	"math"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
)

func baseTestConfig() Config {
	return applyThresholdDefaults(Config{
		SnapshotRetentionMs: hoursToMs(defaultSnapshotRetentionHours),
		MaxSnapshotsToKeep:  defaultMaxSnapshotsToKeep,
	})
}

func TestResolveTableConfigNoProperties(t *testing.T) {
	base := baseTestConfig()

	got := resolveTableConfig(base, iceberg.Properties{})
	if got != base {
		t.Errorf("expected config untouched, got %+v", got)
	}

	if got := resolveTableConfig(base, nil); got != base {
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
	})

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
	got := resolveTableConfig(baseTestConfig(), iceberg.Properties{propMaxSnapshotAgeMs: "1"})
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
			got := resolveTableConfig(base, iceberg.Properties{propTargetFileSize: value})
			if got.TargetFileSizeBytes != base.TargetFileSizeBytes {
				t.Errorf("expected fallback to %d, got %d", base.TargetFileSizeBytes, got.TargetFileSizeBytes)
			}
		})
	}
}

func TestResolveTableConfigTrimsWhitespace(t *testing.T) {
	got := resolveTableConfig(baseTestConfig(), iceberg.Properties{propTargetFileSize: "  536870912\n"})
	if got.TargetFileSizeBytes != 536870912 {
		t.Errorf("expected TargetFileSizeBytes=536870912, got %d", got.TargetFileSizeBytes)
	}
}

func TestResolveTableConfigLeavesOtherFields(t *testing.T) {
	base := baseTestConfig()
	base.Operations = "compact"
	base.Where = "day = 3"
	base.MinInputFiles = 9

	got := resolveTableConfig(base, iceberg.Properties{propTargetFileSize: "536870912"})
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
