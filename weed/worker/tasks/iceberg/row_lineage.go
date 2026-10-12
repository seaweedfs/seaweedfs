package iceberg

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/parquet-go/parquet-go"
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

// ---------------------------------------------------------------------------------------------- compaction rows

// lineageSource is what inheritance gives the rows of one input data file.
type lineageSource struct {
	firstRowID *int64 // the file's first_row_id; nil: its rows have no row ID to carry (a null is written)
	dataSeq    *int64 // the data sequence number of the file's manifest entry
}

func lineageSourceOf(entry iceberg.ManifestEntry) lineageSource {
	src := lineageSource{dataSeq: manifestEntrySeqNum(entry)}
	if id := entry.DataFile().FirstRowID(); id != nil {
		v := *id
		src.firstRowID = &v
	}
	return src
}

// lineageKind: 1 for _row_id, 2 for _last_updated_sequence_number, 0 for a data column. A column is a lineage column by
// its reserved field id, or by its reserved name when it has no field id; a reserved name under another field id is an
// error (kept as data, the output would have two columns of that name).
func lineageKind(f parquet.Field) (int, error) {
	switch f.ID() {
	case iceberg.RowIDFieldID:
		return 1, nil
	case iceberg.LastUpdatedSequenceNumberFieldID:
		return 2, nil
	}
	named := 0
	switch f.Name() {
	case iceberg.RowIDColumnName:
		named = 1
	case iceberg.LastUpdatedSequenceNumberColumnName:
		named = 2
	}
	if named != 0 && f.ID() != 0 {
		return 0, fmt.Errorf("column %s has field id %d, not the reserved id of that metadata column", f.Name(), f.ID())
	}
	return named, nil
}

// lineageLayout is the output of a v3 compaction: the reference file's own columns in their order (any lineage columns
// it has set aside), then _row_id and _last_updated_sequence_number, optional longs with the spec's field ids.
type lineageLayout struct {
	schema      *parquet.Schema
	reference   *parquet.Schema // the schema the layout was built from (the bin's first file)
	baseFields  []parquet.Field
	baseOffsets []int // output leaf index of each base field's first leaf
	baseLeaves  int
}

func newLineageLayout(reference *parquet.Schema) (*lineageLayout, error) {
	var base []parquet.Field
	var offsets []int
	leaves := 0
	for _, f := range reference.Fields() {
		kind, err := lineageKind(f)
		if err != nil {
			return nil, err
		}
		if kind != 0 {
			continue
		}
		base = append(base, f)
		offsets = append(offsets, leaves)
		leaves += leafCount(f)
	}
	fields := append(append([]parquet.Field{}, base...),
		lineageField{Node: parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int64Type)), iceberg.RowIDFieldID), name: iceberg.RowIDColumnName},
		lineageField{Node: parquet.FieldID(parquet.Optional(parquet.Leaf(parquet.Int64Type)), iceberg.LastUpdatedSequenceNumberFieldID), name: iceberg.LastUpdatedSequenceNumberColumnName},
	)
	return &lineageLayout{
		schema:      parquet.NewSchema(reference.Name(), &orderedGroup{Node: reference, fields: fields}),
		reference:   reference,
		baseFields:  base,
		baseOffsets: offsets,
		baseLeaves:  leaves,
	}, nil
}

// mapperFor matches an input file to the layout: its non-lineage columns must be the reference's, in the same order
// (what the merge required of whole schemas before); its lineage columns, if any, are read wherever they are.
func (l *lineageLayout) mapperFor(input *parquet.Schema, src lineageSource) (*lineageMapper, error) {
	m := &lineageMapper{rowIDLeaf: -1, seqLeaf: -1, outRowID: l.baseLeaves, outSeq: l.baseLeaves + 1, src: src}
	next, base := 0, 0
	for _, f := range input.Fields() {
		n := leafCount(f)
		kind, err := lineageKind(f)
		if err != nil {
			return nil, err
		}
		if kind != 0 {
			if n != 1 || !f.Leaf() {
				return nil, fmt.Errorf("lineage column %s is not a primitive", f.Name())
			}
			target := &m.rowIDLeaf
			if kind == 2 {
				target = &m.seqLeaf
			}
			if *target >= 0 {
				return nil, fmt.Errorf("more than one %s column", f.Name())
			}
			*target = next
			m.leafMap = append(m.leafMap, -1)
			next++
			continue
		}
		if base >= len(l.baseFields) || f.Name() != l.baseFields[base].Name() || !parquet.EqualNodes(f, l.baseFields[base]) {
			return nil, fmt.Errorf("cannot merge files with different schemas")
		}
		for i := 0; i < n; i++ {
			m.leafMap = append(m.leafMap, l.baseOffsets[base]+i)
		}
		next += n
		base++
	}
	if base != len(l.baseFields) {
		return nil, fmt.Errorf("cannot merge files with different schemas")
	}
	return m, nil
}

// lineageMapper turns one input file's rows into the layout's rows.
type lineageMapper struct {
	leafMap          []int // input leaf index -> output leaf index; -1 for an input lineage column
	rowIDLeaf        int   // input leaf index of _row_id, -1 when the file has none
	seqLeaf          int   // input leaf index of _last_updated_sequence_number, -1 when the file has none
	outRowID, outSeq int
	src              lineageSource
	rows             []parquet.Row   // reused per batch: both writers copy the values they are handed
	values           []parquet.Value // backing store of rows
}

// mapRows maps rows read at the given positions of the input file (their position before any delete applied).
func (m *lineageMapper) mapRows(rows []parquet.Row, positions []int64) []parquet.Row {
	need := 0
	for _, row := range rows {
		need += len(row) + 2
	}
	if cap(m.values) < need {
		m.values = make([]parquet.Value, 0, need)
	}
	m.values = m.values[:0]
	m.rows = m.rows[:0]
	for i, row := range rows {
		start := len(m.values)
		mapped := m.values[start:start:cap(m.values)]
		var rowID, seq *parquet.Value
		for j := range row {
			v := row[j]
			switch c := v.Column(); {
			case c == m.rowIDLeaf:
				rowID = &row[j]
			case c == m.seqLeaf:
				seq = &row[j]
			default:
				mapped = append(mapped, v.Level(v.RepetitionLevel(), v.DefinitionLevel(), m.leafMap[c]))
			}
		}
		var inheritedRowID *int64
		if m.src.firstRowID != nil {
			id := *m.src.firstRowID + positions[i]
			inheritedRowID = &id
		}
		mapped = append(mapped, lineageValue(rowID, inheritedRowID, m.outRowID), lineageValue(seq, m.src.dataSeq, m.outSeq))
		m.values = m.values[:start+len(mapped)]
		m.rows = append(m.rows, parquet.Row(mapped))
	}
	return m.rows
}

// lineageValue: the value the file materializes when it is not null, else the inherited one, else a null.
func lineageValue(materialized *parquet.Value, inherited *int64, column int) parquet.Value {
	if materialized != nil && !materialized.IsNull() {
		return parquet.Int64Value(materialized.Int64()).Level(0, 1, column)
	}
	if inherited != nil {
		return parquet.Int64Value(*inherited).Level(0, 1, column)
	}
	return parquet.NullValue().Level(0, 0, column)
}

func leafCount(n parquet.Node) int {
	if n.Leaf() {
		return 1
	}
	total := 0
	for _, f := range n.Fields() {
		total += leafCount(f)
	}
	return total
}

// orderedGroup is a group node with the fields given, in that order (parquet.Group sorts its fields by name).
type orderedGroup struct {
	parquet.Node
	fields []parquet.Field
}

func (g *orderedGroup) Fields() []parquet.Field { return g.fields }
func (g *orderedGroup) Leaf() bool              { return false }

// GoType: rows are written as parquet.Row, never from Go values, so the base's type is never read for these columns.
func (g *orderedGroup) GoType() reflect.Type { return g.Node.GoType() }

// lineageField names a lineage column in the layout.
type lineageField struct {
	parquet.Node
	name string
}

func (f lineageField) Name() string                           { return f.name }
func (f lineageField) Value(base reflect.Value) reflect.Value { return reflect.Value{} }

// prepareLineageInput: below v3 (layout nil) the file is merged as it is, with the equality-delete columns resolved on
// the first file. On v3 the file is mapped onto the layout, and its equality-delete columns are resolved on its own
// schema, since a file's lineage columns may sit anywhere among its columns.
func prepareLineageInput(layout *lineageLayout, reader *parquet.Reader, entry iceberg.ManifestEntry, firstFileEqGroups []resolvedEqDeleteGroup,
	eqDeleteGroups []equalityDeleteGroup, icebergSchema *iceberg.Schema) (*lineageMapper, []resolvedEqDeleteGroup, error) {
	if layout == nil {
		return nil, firstFileEqGroups, nil
	}
	source := entry.DataFile().FilePath()
	mapper, err := layout.mapperFor(reader.Schema(), lineageSourceOf(entry))
	if err != nil {
		return nil, nil, fmt.Errorf("schema mismatch in %s: %w", source, err)
	}
	if reader.Schema() == layout.reference {
		return mapper, firstFileEqGroups, nil // resolved on this very schema already
	}
	eqGroups, err := resolveEqualityDeleteGroupsForSchema(reader.Schema(), eqDeleteGroups, icebergSchema)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve equality columns in %s: %w", source, err)
	}
	return mapper, eqGroups, nil
}

// copyFileRows drains one input file into write: deletes filtered and, on a v3 table (layout set), mapped onto the
// layout with each row's lineage. Both merges, bin-packed and sorted, use it.
func copyFileRows(ctx context.Context, reader *parquet.Reader, entry iceberg.ManifestEntry, layout *lineageLayout, bucketName, dataPath string,
	positionDeletes map[string][]int64, firstFileEqGroups []resolvedEqDeleteGroup, eqDeleteGroups []equalityDeleteGroup,
	icebergSchema *iceberg.Schema, write func([]parquet.Row) error) (int64, error) {
	mapper, eqGroups, err := prepareLineageInput(layout, reader, entry, firstFileEqGroups, eqDeleteGroups, icebergSchema)
	if err != nil {
		reader.Close()
		return 0, err
	}
	return visitFilteredParquetRows(ctx, reader, entry.DataFile().FilePath(), bucketName, dataPath, positionDeletes, eqGroups, func(filtered []parquet.Row, positions []int64) error {
		if mapper != nil {
			filtered = mapper.mapRows(filtered, positions)
		}
		return write(filtered)
	})
}

// mergedFileFirstRowID is the _row_id of the merged output's first row, or nil when it carries none: the value the
// manifest entry's first_row_id must name, since rewritten rows keep their IDs rather than taking fresh ones.
func mergedFileFirstRowID(data []byte) (*int64, error) {
	reader := parquet.NewReader(bytes.NewReader(data))
	defer reader.Close()
	schema := reader.Schema()
	if schema == nil {
		return nil, nil
	}
	leaf, ok := schema.Lookup(iceberg.RowIDColumnName)
	if !ok {
		return nil, nil
	}
	rows := make([]parquet.Row, 1)
	n, err := reader.ReadRows(rows)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	for _, v := range rows[0] {
		if v.Column() != leaf.ColumnIndex {
			continue
		}
		if v.IsNull() {
			return nil, nil
		}
		id := v.Int64()
		return &id, nil
	}
	return nil, nil
}
