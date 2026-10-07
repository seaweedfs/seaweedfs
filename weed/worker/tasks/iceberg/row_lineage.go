package iceberg

import (
	"fmt"
	"io"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Row lineage (Iceberg v3, spec "Row Lineage" and "Snapshot Row IDs").
//
// Every snapshot committed to a v3 table carries first-row-id, the table's next-row-id when the commit is attempted,
// and added-rows, the upper bound of the row IDs its manifest list assigns; committing advances next-row-id by
// added-rows. iceberg-go refuses a v3 snapshot without them ("invalid row lineage: first-row-id is required for v3
// snapshots"), which is what every maintenance commit on a v3 table hit. The manifest list assigns first_row_id to the
// data manifests that have none, starting at the snapshot's first-row-id; passing 0 there (as the maintenance code did)
// would hand rows IDs other rows already hold.
//
// A rewrite that moves rows into a new data file (compaction) copies each row's _row_id and
// _last_updated_sequence_number into it: the value the file materializes, else the one inheritance gives it
// (the input file's first_row_id plus the row's position; the input entry's data sequence number). A compacted row keeps
// its identity, and a rewrite that modifies no row keeps its last-updated sequence number.

const minFormatVersionRowLineage = 3

func hasRowLineage(version int) bool { return version >= minFormatVersionRowLineage }

// rowLineageFirstRowID is the first-row-id a snapshot planned on meta starts at (0 below v3, where it is unused).
func rowLineageFirstRowID(meta table.Metadata) int64 {
	if !hasRowLineage(meta.Version()) {
		return 0
	}
	return meta.NextRowID()
}

// checkRowLineagePlan: a plan that assigned row IDs from next-row-id `planned` is stale once the table's next-row-id
// moved, even if the head did not (a commit to another branch advances it too); the spec has first-row-id reassigned on
// every attempt, which for a manifest list already written means planning again.
//
// A format-version change since planning (an upgrade to v3) is stale too: the manifests and the snapshot were built for
// the old version.
func checkRowLineagePlan(plannedVersion, currentVersion int, planned, current int64) error {
	if currentVersion != plannedVersion {
		return fmt.Errorf("%w: format version changed from %d to %d", errStalePlan, plannedVersion, currentVersion)
	}
	if hasRowLineage(plannedVersion) && planned != current {
		return fmt.Errorf("%w: next-row-id moved from %d to %d", errStalePlan, planned, current)
	}
	return nil
}

// checkCommitPlan is what every maintenance commit re-checks against the metadata it commits onto: the head it planned
// against, and the row-lineage state (format version, next-row-id) its manifest list was written for.
func checkCommitPlan(currentMeta table.Metadata, snapshotID int64, version int, firstRowID int64) (*table.Snapshot, error) {
	cs := currentMeta.CurrentSnapshot()
	if cs == nil || cs.SnapshotID != snapshotID {
		return nil, errStalePlan
	}
	if err := checkRowLineagePlan(version, currentMeta.Version(), firstRowID, rowLineageFirstRowID(currentMeta)); err != nil {
		return nil, err
	}
	return cs, nil
}

// writeManifestList writes the snapshot's manifest list and returns its added-rows for a v3 table (nil below v3): the row
// IDs the writer assigned to manifests without a first_row_id, starting at firstRowID.
func writeManifestList(version int, out io.Writer, snapshotID int64, parentSnapshotID, sequenceNumber *int64, firstRowID int64, manifests []iceberg.ManifestFile) (*int64, error) {
	if !hasRowLineage(version) {
		return nil, iceberg.WriteManifestList(version, out, snapshotID, parentSnapshotID, sequenceNumber, firstRowID, manifests)
	}
	if sequenceNumber == nil {
		return nil, fmt.Errorf("sequence number is required for v%d tables", version)
	}
	writer, err := iceberg.NewManifestListWriterV3(out, snapshotID, *sequenceNumber, firstRowID, parentSnapshotID)
	if err != nil {
		return nil, err
	}
	if err := writer.AddManifests(manifests); err != nil {
		writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	next := writer.NextRowID()
	if next == nil || *next < firstRowID {
		return nil, fmt.Errorf("manifest list writer reported next-row-id %v below first-row-id %d", next, firstRowID)
	}
	addedRows := *next - firstRowID
	return &addedRows, nil
}

// setRowLineage puts first-row-id and added-rows on a snapshot of a v3 table.
func setRowLineage(snapshot *table.Snapshot, version int, firstRowID int64, addedRows *int64) {
	if !hasRowLineage(version) || addedRows == nil {
		return
	}
	first, added := firstRowID, *addedRows
	snapshot.FirstRowID, snapshot.AddedRows = &first, &added
}

