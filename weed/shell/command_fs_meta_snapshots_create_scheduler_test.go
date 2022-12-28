package shell

import (
	"github.com/stretchr/testify/assert"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestComputeRequirementsFromDirectory(t *testing.T) {
	homeDir := "home"
	snapDir := "test"
	// case1: we have previous snapshots, target date is relative close to the latest snapshot date, we will use previous snapshots to generate future snapshots and remove some previous snapshots.
	var prevSnapshots []string
	prevSnapshots = append(prevSnapshots, "2022-01-01")
	prevSnapshots = append(prevSnapshots, "2022-02-01")
	prevSnapshots = append(prevSnapshots, "2022-03-01")
	targetDate, _ := time.Parse(DateFormat, "2022-04-01")
	targetDate = targetDate.Add(-1 * time.Nanosecond)
	snapshotsToRemove, snapshotsToGenerate, levelDbBootstrapPath, err := computeRequirementsFromDirectory(prevSnapshots, homeDir, snapDir, 3, 15, targetDate)
	assert.Nil(t, err)
	assert.Equal(t, levelDbBootstrapPath, "2022-03-01", "latest previous snapshot should be 2022-03-01")
	expectedToRemoveSnapshots := []string{filepath.Join(homeDir, snapDir, "2022-01-01"), filepath.Join(homeDir, snapDir, "2022-02-01")}
	assert.True(t, reflect.DeepEqual(snapshotsToRemove, expectedToRemoveSnapshots))
	expectedSnapshotsGenerationDate := []string{"2022-03-16", "2022-03-31"}
	for i, date := range expectedSnapshotsGenerationDate {
		assert.Equal(t, snapshotsToGenerate[i].Format(DateFormat), date)
	}

	// case2: we have previous snapshots, target date is too close to the latest snapshot date, no change will happen.
	targetDate, _ = time.Parse(DateFormat, "2022-03-02")
	snapshotsToRemove, snapshotsToGenerate, levelDbBootstrapPath, err = computeRequirementsFromDirectory(prevSnapshots, "home", "test", 3, 15, targetDate)
	assert.NotNil(t, err)
	assert.Containsf(t, err.Error(), "no need to generate new snapshots", "expected error containing %q, got %s", "no need to generate new snapshots", err)
	assert.Empty(t, snapshotsToRemove)
	assert.Empty(t, snapshotsToGenerate)
	assert.Equal(t, levelDbBootstrapPath, "2022-03-01", "latest previous snapshot should be 2022-03-01")

	// case3: we have previous snapshots, target date is too far out to the latest snapshot date, all previous snapshots will be removed, snapshots will be generated going backward starting on target date.
	targetDate, _ = time.Parse(DateFormat, "2022-12-01")
	targetDate = targetDate.Add(-1 * time.Nanosecond)
	snapshotsToRemove, snapshotsToGenerate, levelDbBootstrapPath, err = computeRequirementsFromDirectory(prevSnapshots, "home", "test", 3, 15, targetDate)
	assert.Nil(t, err)
	expectedToRemoveSnapshots = []string{filepath.Join(homeDir, snapDir, "2022-01-01"), filepath.Join(homeDir, snapDir, "2022-02-01"), filepath.Join(homeDir, snapDir, "2022-03-01")}
	assert.True(t, reflect.DeepEqual(snapshotsToRemove, expectedToRemoveSnapshots))
	// we still need to skip all logs prior to 2022-03-02
	assert.Equal(t, levelDbBootstrapPath, "2022-03-01", "latest previous snapshot should be 2022-03-01")
	expectedSnapshotsGenerationDate = []string{"2022-11-30", "2022-11-15", "2022-10-31"}
	for i, date := range expectedSnapshotsGenerationDate {
		assert.Equal(t, snapshotsToGenerate[i].Format(DateFormat), date)
	}

	// case4: the target date is exactly n snapshots away from previous snapshot date
	targetDate, _ = time.Parse(DateFormat, "2022-03-04")
	targetDate = targetDate.Add(-1 * time.Nanosecond)
	snapshotsToRemove, snapshotsToGenerate, levelDbBootstrapPath, err = computeRequirementsFromDirectory(prevSnapshots, "home", "test", 3, 1, targetDate)
	expectedToRemoveSnapshots = []string{filepath.Join(homeDir, snapDir, "2022-01-01"), filepath.Join(homeDir, snapDir, "2022-02-01")}
	assert.True(t, reflect.DeepEqual(snapshotsToRemove, expectedToRemoveSnapshots))
	expectedSnapshotsGenerationDate = []string{"2022-03-02", "2022-03-03"}
	for i, date := range expectedSnapshotsGenerationDate {
		assert.Equal(t, snapshotsToGenerate[i].Format(DateFormat), date)
	}
}
