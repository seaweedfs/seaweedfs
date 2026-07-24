package iceberg

import (
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Snapshot summary property names from the Iceberg spec. iceberg-go keeps its
// own copies unexported, so the maintenance operations spell them here.
const (
	summaryAddedDataFiles      = "added-data-files"
	summaryDeletedDataFiles    = "deleted-data-files"
	summaryAddedDeleteFiles    = "added-delete-files"
	summaryRemovedDeleteFiles  = "removed-delete-files"
	summaryAddedPosDeleteFiles = "added-position-delete-files"
	summaryRemovedPosDelFiles  = "removed-position-delete-files"
	summaryAddedEqDeleteFiles  = "added-equality-delete-files"
	summaryRemovedEqDelFiles   = "removed-equality-delete-files"
	summaryAddedRecords        = "added-records"
	summaryDeletedRecords      = "deleted-records"
	summaryAddedPosDeletes     = "added-position-deletes"
	summaryRemovedPosDeletes   = "removed-position-deletes"
	summaryAddedEqDeletes      = "added-equality-deletes"
	summaryRemovedEqDeletes    = "removed-equality-deletes"
	summaryAddedFilesSize      = "added-files-size"
	summaryRemovedFilesSize    = "removed-files-size"

	summaryTotalDataFiles   = "total-data-files"
	summaryTotalDeleteFiles = "total-delete-files"
	summaryTotalRecords     = "total-records"
	summaryTotalFilesSize   = "total-files-size"
	summaryTotalPosDeletes  = "total-position-deletes"
	summaryTotalEqDeletes   = "total-equality-deletes"
)

// snapshotSummary accumulates the files a maintenance operation adds to and
// removes from a table, and renders them as an Iceberg snapshot summary.
//
// Engines read a table's size straight out of the current snapshot summary —
// PyIceberg's inspect.snapshots, Trino's $snapshots and Spark's DESCRIBE all
// report total-records, total-data-files and total-files-size verbatim — so a
// maintenance snapshot carrying only its own labels blanks those numbers for
// the whole table until the next writer commits.
type snapshotSummary struct {
	addedFilesSize, removedFilesSize     int64
	addedDataFiles, removedDataFiles     int64
	addedRecords, removedRecords         int64
	addedDeleteFiles, removedDeleteFiles int64
	addedPosDeleteFiles, removedPosFiles int64
	addedEqDeleteFiles, removedEqFiles   int64
	addedPosDeletes, removedPosDeletes   int64
	addedEqDeletes, removedEqDeletes     int64
}

func (s *snapshotSummary) addFile(df iceberg.DataFile) {
	s.addedFilesSize += df.FileSizeBytes()
	switch df.ContentType() {
	case iceberg.EntryContentData:
		s.addedDataFiles++
		s.addedRecords += df.Count()
	case iceberg.EntryContentPosDeletes:
		s.addedDeleteFiles++
		s.addedPosDeleteFiles++
		s.addedPosDeletes += df.Count()
	case iceberg.EntryContentEqDeletes:
		s.addedDeleteFiles++
		s.addedEqDeleteFiles++
		s.addedEqDeletes += df.Count()
	}
}

func (s *snapshotSummary) removeFile(df iceberg.DataFile) {
	s.removedFilesSize += df.FileSizeBytes()
	switch df.ContentType() {
	case iceberg.EntryContentData:
		s.removedDataFiles++
		s.removedRecords += df.Count()
	case iceberg.EntryContentPosDeletes:
		s.removedDeleteFiles++
		s.removedPosFiles++
		s.removedPosDeletes += df.Count()
	case iceberg.EntryContentEqDeletes:
		s.removedDeleteFiles++
		s.removedEqFiles++
		s.removedEqDeletes += df.Count()
	}
}

// build renders the summary for a snapshot replacing parent: the operation's
// own labels, the added/removed counters, and the running totals.
//
// A total is carried forward only when the parent recorded it. Iceberg treats a
// missing total as zero, which turns a compaction that replaces two files with
// one into "total-data-files: -1" — dropped as negative — or worse, a table
// with millions of rows into "total-records: 0". Leaving the field out lets a
// reader fall back to the manifests instead of believing a made-up number.
func (s snapshotSummary) build(operation table.Operation, parent *table.Snapshot, labels map[string]string) *table.Summary {
	props := iceberg.Properties{}
	for k, v := range labels {
		props[k] = v
	}

	setPositive := func(key string, value int64) {
		if value > 0 {
			props[key] = strconv.FormatInt(value, 10)
		}
	}
	setPositive(summaryAddedFilesSize, s.addedFilesSize)
	setPositive(summaryRemovedFilesSize, s.removedFilesSize)
	setPositive(summaryAddedDataFiles, s.addedDataFiles)
	setPositive(summaryDeletedDataFiles, s.removedDataFiles)
	setPositive(summaryAddedRecords, s.addedRecords)
	setPositive(summaryDeletedRecords, s.removedRecords)
	setPositive(summaryAddedDeleteFiles, s.addedDeleteFiles)
	setPositive(summaryRemovedDeleteFiles, s.removedDeleteFiles)
	setPositive(summaryAddedPosDeleteFiles, s.addedPosDeleteFiles)
	setPositive(summaryRemovedPosDelFiles, s.removedPosFiles)
	setPositive(summaryAddedEqDeleteFiles, s.addedEqDeleteFiles)
	setPositive(summaryRemovedEqDelFiles, s.removedEqFiles)
	setPositive(summaryAddedPosDeletes, s.addedPosDeletes)
	setPositive(summaryRemovedPosDeletes, s.removedPosDeletes)
	setPositive(summaryAddedEqDeletes, s.addedEqDeletes)
	setPositive(summaryRemovedEqDeletes, s.removedEqDeletes)

	var previous iceberg.Properties
	if parent != nil && parent.Summary != nil {
		previous = parent.Summary.Properties
	}
	carryTotal := func(key string, added, removed int64) {
		raw, ok := previous[key]
		if !ok {
			return
		}
		before, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return
		}
		if total := before + added - removed; total >= 0 {
			props[key] = strconv.FormatInt(total, 10)
		}
	}
	carryTotal(summaryTotalDataFiles, s.addedDataFiles, s.removedDataFiles)
	carryTotal(summaryTotalDeleteFiles, s.addedDeleteFiles, s.removedDeleteFiles)
	carryTotal(summaryTotalRecords, s.addedRecords, s.removedRecords)
	carryTotal(summaryTotalFilesSize, s.addedFilesSize, s.removedFilesSize)
	carryTotal(summaryTotalPosDeletes, s.addedPosDeletes, s.removedPosDeletes)
	carryTotal(summaryTotalEqDeletes, s.addedEqDeletes, s.removedEqDeletes)

	return &table.Summary{Operation: operation, Properties: props}
}
