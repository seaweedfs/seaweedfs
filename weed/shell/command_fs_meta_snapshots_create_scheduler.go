package shell

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func computeRequirementsFromDirectory(previousSnapshots []string, homeDirectory string, snapshotPath string, count int, durationDays int, targetDate time.Time) (snapshotsToRemove []string, snapshotsToGenerate []time.Time, levelDbBootstrapPath string, err error) {
	levelDbBootstrapPath = previousSnapshots[len(previousSnapshots)-1]
	lastSnapshotDate, err := time.Parse(DateFormat, levelDbBootstrapPath[:len(DateFormat)])
	if err != nil {
		return
	}
	if err != nil {
		return
	}
	// the snapshots cover the last nanosecond of the current date
	lastSnapshotDate = lastSnapshotDate.AddDate(0, 0, 1).Add(-1 * time.Nanosecond)
	gapDuration := targetDate.Sub(lastSnapshotDate)
	oneSnapshotInterval := 24 * time.Hour * time.Duration(durationDays)
	totalSnapshotsInterval := 24 * time.Hour * time.Duration(durationDays*count)
	// gap too small no snapshot will be generated
	if gapDuration < oneSnapshotInterval {
		return snapshotsToRemove, snapshotsToGenerate, levelDbBootstrapPath, errors.New(fmt.Sprintf("last snapshot was generated at %v no need to generate new snapshots", lastSnapshotDate.Format(DateFormat)))
	} else if gapDuration > totalSnapshotsInterval {
		// gap too large generate from targetDate
		// and remove all previous snapshots
		_, snapshotsToGenerate, _, err = computeRequirementsFromEmpty(count, durationDays, targetDate)
		for _, file := range previousSnapshots {
			snapshotsToRemove = append(snapshotsToRemove, filepath.Join(homeDirectory, snapshotPath, file))
		}
		return
	}
	snapshotDate := lastSnapshotDate.AddDate(0, 0, durationDays)
	for snapshotDate.Before(targetDate) || snapshotDate.Equal(targetDate) {
		snapshotsToGenerate = append(snapshotsToGenerate, snapshotDate)
		snapshotDate = snapshotDate.AddDate(0, 0, durationDays)
	}
	totalCount := len(previousSnapshots) + len(snapshotsToGenerate)
	toRemoveIdx := 0
	for toRemoveIdx < len(previousSnapshots) && totalCount-toRemoveIdx > count {
		snapshotsToRemove = append(snapshotsToRemove, filepath.Join(homeDirectory, snapshotPath, previousSnapshots[toRemoveIdx]))
		toRemoveIdx += 1
	}
	return
}

func computeRequirementsFromEmpty(count int, durationDays int, targetDate time.Time) (snapshotsToRemove []string, snapshotsToGenerate []time.Time, levelDbBootstrapPath string, err error) {
	snapshotDate := targetDate
	for i := 0; i < count; i++ {
		snapshotsToGenerate = append(snapshotsToGenerate, snapshotDate)
		snapshotDate = snapshotDate.AddDate(0, 0, -1*durationDays)
	}
	return snapshotsToRemove, snapshotsToGenerate, "", nil
}

// compute number of snapshot need to be generated and number of snapshots to remove from give directory.
func computeRequirements(homeDirectory string, snapshotPath string, count int, durationDays int) (snapshotsToRemove []string, snapshotsToGenerate []time.Time, levelDbBootstrapPath string, err error) {
	snapshotDirectory := filepath.Join(homeDirectory, snapshotPath)
	files, _ := os.ReadDir(snapshotDirectory)
	// sort files by name
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})
	// filter for snapshots file name only
	var prevSnapshotFiles []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), SnapshotDirPostFix) {
			prevSnapshotFiles = append(prevSnapshotFiles, file.Name())
		}
	}
	curDate := time.Now()
	curDateStr := curDate.Format(DateFormat)
	// ensure snapshot start at today 00:00 - 1ns
	today, err := time.Parse(DateFormat, curDateStr)
	targetDate := today.Add(-1 * time.Nanosecond)
	if len(prevSnapshotFiles) == 0 {
		return computeRequirementsFromEmpty(count, durationDays, targetDate)
	}
	return computeRequirementsFromDirectory(prevSnapshotFiles, homeDirectory, snapshotPath, count, durationDays, targetDate)
}
