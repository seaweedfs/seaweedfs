package iceberg

import (
	"bytes"
	"context"
	"io"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

// testDataSeq and testDeleteSeq are the data sequence numbers of the unit
// tests' hand-built entries: the delete files committed after the data.
var testDataSeq, testDeleteSeq = int64(1), int64(2)

// testUnpartitionedSpecs is the spec list of the unit tests' unpartitioned tables.
var testUnpartitionedSpecs = map[int]iceberg.PartitionSpec{0: *iceberg.UnpartitionedSpec}

type idNameRow struct {
	ID   int64  `parquet:"id"`
	Name string `parquet:"name"`
}

func readIDNameRows(t *testing.T, content []byte) []idNameRow {
	t.Helper()
	reader := parquet.NewReader(bytes.NewReader(content))
	defer reader.Close()
	var rows []idNameRow
	for {
		var r idNameRow
		if err := reader.Read(&r); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("read: %v", err)
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

// An upsert writer (Flink, the Iceberg Kafka Connect sink, CDC tools) commits
// an update as the row's new version plus an equality delete of its key, in
// one commit. The delete applies only to data files with a lower data sequence
// number: the version written with it, and any written later, stay. Applying
// every key to every file drops both versions of every updated row.
func TestCompactionAppliesEqualityDeletesOnlyToOlderDataFiles(t *testing.T) {
	skipIfEqualityDeleteAvroUnsupported(t)
	fs, client := startFakeFiler(t)

	setup := tableSetup{BucketName: "tb", Namespace: "ns", TableName: "tbl",
		// d1 was committed before the deletes (1), d2 with them (2), d3 after them (3)
		DataSequenceNumbers: map[string]int64{"d2.parquet": 2, "d3.parquet": 3}}
	type rows = []struct {
		ID   int64
		Name string
	}
	populateTableWithDeleteFiles(t, fs, setup,
		[]struct {
			Name string
			Rows rows
		}{
			{"d1.parquet", rows{{1, "alice"}, {2, "bob"}, {3, "carol"}}},
			{"d2.parquet", rows{{1, "alice v2"}}},
			{"d3.parquet", rows{{2, "bob v3"}}},
		},
		nil,
		[]struct {
			Name     string
			FieldIDs []int
			Rows     rows
		}{
			{"ed1.parquet", []int{1}, rows{{1, ""}, {2, ""}}}, // field 1 = id
		},
	)

	handler := NewHandler(nil)
	config := Config{TargetFileSizeBytes: 256 * 1024 * 1024, MinInputFiles: 2, MaxCommitRetries: 3, ApplyDeletes: true}
	result, _, err := handler.compactDataFiles(context.Background(), client, setup.BucketName, setup.tablePath(), config, nil)
	if err != nil {
		t.Fatalf("compactDataFiles: %v", err)
	}
	if !strings.Contains(result, "compacted") {
		t.Fatalf("expected compaction result, got %q", result)
	}

	var merged []byte
	for _, e := range fs.listDir(path.Join(s3tables.TablesPath, setup.BucketName, setup.tablePath(), "data")) {
		if strings.HasPrefix(e.Name, "compact-") {
			merged = e.Content
		}
	}
	if merged == nil {
		t.Fatal("no merged file found")
	}
	got := readIDNameRows(t, merged)
	want := []idNameRow{{1, "alice v2"}, {2, "bob v3"}, {3, "carol"}}
	if len(got) != len(want) {
		t.Fatalf("merged rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("merged rows = %v, want %v", got, want)
		}
	}
}

func putEqDeleteFile(t *testing.T, fs *fakeFilerServer, name string, rows []idNameRow) int64 {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, parquet.SchemaOf(new(idNameRow)))
	for i := range rows {
		if err := w.Write(&rows[i]); err != nil {
			t.Fatalf("write eq delete: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close eq delete: %v", err)
	}
	fs.putEntry("/buckets/test-bucket/ns/tbl/data", name, &filer_pb.Entry{Name: name, Content: buf.Bytes()})
	return int64(buf.Len())
}

func putIDNameDataFile(t *testing.T, fs *fakeFilerServer, name string, rows []idNameRow) int64 {
	t.Helper()
	in := make([]struct {
		ID   int64
		Name string
	}, len(rows))
	for i, r := range rows {
		in[i].ID, in[i].Name = r.ID, r.Name
	}
	return int64(len(writeTestParquetFile(t, fs, "/buckets/test-bucket/ns/tbl/data", name, in)))
}

func entryWithSeq(t *testing.T, spec iceberg.PartitionSpec, content iceberg.ManifestEntryContent, filePath string,
	partition map[int]any, records, size int64, seq *int64, eqFieldIDs []int) iceberg.ManifestEntry {
	t.Helper()
	dfb, err := iceberg.NewDataFileBuilder(spec, content, filePath, iceberg.ParquetFile, partition, nil, nil, records, size)
	if err != nil {
		t.Fatalf("build %s: %v", filePath, err)
	}
	if eqFieldIDs != nil {
		dfb.EqualityFieldIDs(eqFieldIDs)
	}
	snapID := int64(1)
	return iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &snapID, seq, seq, dfb.Build())
}

// An equality delete file applies only within its own partition, unless its
// partition spec is unpartitioned (Iceberg spec, "Scan Planning").
func TestEqualityDeletesApplyOnlyWithinTheirPartition(t *testing.T) {
	fs, client := startFakeFiler(t)
	schema := newTestSchema()
	spec := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "name", Transform: iceberg.IdentityTransform{}})
	specs := map[int]iceberg.PartitionSpec{0: *iceberg.UnpartitionedSpec, 1: spec}
	one, two := int64(1), int64(2)

	sizeA := putIDNameDataFile(t, fs, "pa.parquet", []idNameRow{{1, "a"}, {2, "a"}})
	sizeB := putIDNameDataFile(t, fs, "pb.parquet", []idNameRow{{1, "b"}})
	dataA := entryWithSeq(t, spec, iceberg.EntryContentData, "data/pa.parquet", map[int]any{1000: "a"}, 2, sizeA, &one, nil)
	dataB := entryWithSeq(t, spec, iceberg.EntryContentData, "data/pb.parquet", map[int]any{1000: "b"}, 1, sizeB, &one, nil)

	// a delete of id 1 in partition a
	sizeDel := putEqDeleteFile(t, fs, "eq-a.parquet", []idNameRow{{1, ""}})
	delA := entryWithSeq(t, spec, iceberg.EntryContentEqDeletes, "data/eq-a.parquet", map[int]any{1000: "a"}, 1, sizeDel, &two, []int{1})
	groups, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{delA}, schema, specs)
	if err != nil {
		t.Fatalf("collectEqualityDeletes: %v", err)
	}
	// as the compaction does: the groups narrowed to the bin's partition, then merged
	merge := func(entry iceberg.ManifestEntry, groups []equalityDeleteGroup) []idNameRow {
		bin := compactionBin{SpecID: entry.DataFile().SpecID(), Partition: entry.DataFile().Partition(), Entries: []iceberg.ManifestEntry{entry}}
		groups, err := eqDeleteGroupsForBin(groups, bin, specs, schema)
		if err != nil {
			t.Fatalf("eqDeleteGroupsForBin %s: %v", entry.DataFile().FilePath(), err)
		}
		out, _, err := mergeParquetFiles(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{entry}, nil, groups, schema, 0, false)
		if err != nil {
			t.Fatalf("mergeParquetFiles %s: %v", entry.DataFile().FilePath(), err)
		}
		return readIDNameRows(t, out)
	}
	if got := merge(dataA, groups); len(got) != 1 || got[0] != (idNameRow{2, "a"}) {
		t.Errorf("partition a = %v, want only {2 a}", got)
	}
	if got := merge(dataB, groups); len(got) != 1 || got[0] != (idNameRow{1, "b"}) {
		t.Errorf("partition b = %v, want {1 b} kept: the delete is partition a's", got)
	}

	// the same delete from an unpartitioned delete file applies to every partition
	sizeGlobal := putEqDeleteFile(t, fs, "eq-all.parquet", []idNameRow{{1, ""}})
	delAll := entryWithSeq(t, *iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes, "data/eq-all.parquet", map[int]any{}, 1, sizeGlobal, &two, []int{1})
	groups, err = collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{delAll}, schema, specs)
	if err != nil {
		t.Fatalf("collectEqualityDeletes: %v", err)
	}
	if got := merge(dataB, groups); len(got) != 0 {
		t.Errorf("partition b = %v, want nothing: an unpartitioned delete applies everywhere", got)
	}
}

// Without data sequence numbers the order of a delete and a row cannot be
// known, so the compaction refuses rather than guesses.
func TestEqualityDeletesWithoutSequenceNumbersAreRefused(t *testing.T) {
	fs, client := startFakeFiler(t)
	schema := newTestSchema()
	spec := *iceberg.UnpartitionedSpec
	one, two := int64(1), int64(2)

	sizeDel := putEqDeleteFile(t, fs, "eq.parquet", []idNameRow{{1, ""}})
	unsequencedDelete := entryWithSeq(t, spec, iceberg.EntryContentEqDeletes, "data/eq.parquet", map[int]any{}, 1, sizeDel, nil, []int{1})
	if _, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{unsequencedDelete}, schema, testUnpartitionedSpecs); err == nil ||
		!strings.Contains(err.Error(), "sequence number") {
		t.Errorf("collectEqualityDeletes without a sequence number: err = %v, want a sequence number error", err)
	}

	sequencedDelete := entryWithSeq(t, spec, iceberg.EntryContentEqDeletes, "data/eq.parquet", map[int]any{}, 1, sizeDel, &two, []int{1})
	groups, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{sequencedDelete}, schema, testUnpartitionedSpecs)
	if err != nil {
		t.Fatalf("collectEqualityDeletes: %v", err)
	}
	size := putIDNameDataFile(t, fs, "d.parquet", []idNameRow{{1, "x"}, {2, "y"}})
	unsequencedData := entryWithSeq(t, spec, iceberg.EntryContentData, "data/d.parquet", map[int]any{}, 2, size, nil, nil)
	if _, _, err := mergeParquetFiles(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{unsequencedData}, nil, groups, schema, 0, false); err == nil ||
		!strings.Contains(err.Error(), "sequence number") {
		t.Errorf("merge of a data file without a sequence number: err = %v, want a sequence number error", err)
	}
	// control: the same file with its sequence number merges, the delete applied
	sequencedData := entryWithSeq(t, spec, iceberg.EntryContentData, "data/d.parquet", map[int]any{}, 2, size, &one, nil)
	out, _, err := mergeParquetFiles(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{sequencedData}, nil, groups, schema, 0, false)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}
	if got := readIDNameRows(t, out); len(got) != 1 || got[0] != (idNameRow{2, "y"}) {
		t.Errorf("merged = %v, want only {2 y}", got)
	}
}

// A spec whose every field is void (a v1 table's dropped partition field) is
// unpartitioned: a delete under it applies to every data file, whatever the
// data file's spec. Its partition map is not empty
// ({1001: nil}), so the map alone cannot tell.
func TestEqualityDeleteUnderAnAllVoidSpecAppliesEverywhere(t *testing.T) {
	fs, client := startFakeFiler(t)
	schema := newTestSchema()
	byName := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "name", Transform: iceberg.IdentityTransform{}})
	allVoid := iceberg.NewPartitionSpecID(2, iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1001, Name: "name_dropped", Transform: iceberg.VoidTransform{}})
	specs := map[int]iceberg.PartitionSpec{1: byName, 2: allVoid}
	one, two := int64(1), int64(2)

	size := putIDNameDataFile(t, fs, "pb.parquet", []idNameRow{{1, "b"}, {2, "b"}})
	data := entryWithSeq(t, byName, iceberg.EntryContentData, "data/pb.parquet", map[int]any{1000: "b"}, 2, size, &one, nil)
	sizeDel := putEqDeleteFile(t, fs, "eq-void.parquet", []idNameRow{{1, ""}})
	del := entryWithSeq(t, allVoid, iceberg.EntryContentEqDeletes, "data/eq-void.parquet", map[int]any{1001: nil}, 1, sizeDel, &two, []int{1})

	groups, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{del}, schema, specs)
	if err != nil {
		t.Fatalf("collectEqualityDeletes: %v", err)
	}
	out, _, err := mergeParquetFiles(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{data}, nil, groups, schema, 0, false)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}
	if got := readIDNameRows(t, out); len(got) != 1 || got[0] != (idNameRow{2, "b"}) {
		t.Errorf("merged = %v, want only {2 b}: an all-void spec's delete applies to every partition", got)
	}
}

// A where filter narrows the bins, not the deletes that may apply to them: an
// unpartitioned delete file (no field the clause could match) still applies
// to the bins in the filter's partitions.
func TestAWhereFilterKeepsUnpartitionedDeletes(t *testing.T) {
	byName := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "name", Transform: iceberg.IdentityTransform{}})
	specs := map[int]iceberg.PartitionSpec{0: *iceberg.UnpartitionedSpec, 1: byName}
	predicate := &partitionPredicate{Clauses: []whereClause{{Field: "name", Literals: []string{"'b'"}}}}
	two := int64(2)
	cases := []struct {
		name  string
		entry iceberg.ManifestEntry
		want  bool
	}{
		{"unpartitioned delete", entryWithSeq(t, *iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes, "data/u.parquet", map[int]any{}, 1, 1, &two, []int{1}), true},
		{"delete in the filter's partition", entryWithSeq(t, byName, iceberg.EntryContentEqDeletes, "data/b.parquet", map[int]any{1000: "b"}, 1, 1, &two, []int{1}), true},
		{"delete in another partition", entryWithSeq(t, byName, iceberg.EntryContentEqDeletes, "data/a.parquet", map[int]any{1000: "a"}, 1, 1, &two, []int{1}), false},
	}
	for _, c := range cases {
		got, err := deleteInPredicateScope(c.entry, predicate, specs, newTestSchema())
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("%s: in scope = %v, want %v", c.name, got, c.want)
		}
	}
}

// Partition values are compared as the spec types them, not as their Go shape:
// two writers' manifests decode one uuid as uuid.UUID and as [16]byte (or a
// long as int32 and int64), and a delete compared as JSON text misses its own
// partition, so the merged file keeps the rows it deleted. A value that does
// not convert fails closed: the bin is refused.
func TestEqualityDeletesMatchTheirPartitionByTypedValue(t *testing.T) {
	fs, client := startFakeFiler(t)
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Type: iceberg.PrimitiveTypes.Int64, Name: "id", Required: true},
		iceberg.NestedField{ID: 2, Type: iceberg.PrimitiveTypes.String, Name: "name"},
		iceberg.NestedField{ID: 3, Type: iceberg.PrimitiveTypes.UUID, Name: "tenant"},
		iceberg.NestedField{ID: 4, Type: iceberg.PrimitiveTypes.Int64, Name: "region"},
	)
	spec := iceberg.NewPartitionSpecID(1,
		iceberg.PartitionField{SourceIDs: []int{3}, FieldID: 1000, Name: "tenant", Transform: iceberg.IdentityTransform{}},
		iceberg.PartitionField{SourceIDs: []int{4}, FieldID: 1001, Name: "region", Transform: iceberg.IdentityTransform{}})
	specs := map[int]iceberg.PartitionSpec{0: *iceberg.UnpartitionedSpec, 1: spec}
	tenant := uuid.MustParse("8f14e45f-ceea-467f-a0e6-6f4d9c2b1a01")
	one, two := int64(1), int64(2)

	// the data file as one writer's manifest decodes it, the delete as another's
	dataPartition := map[int]any{1000: tenant, 1001: int64(7)}
	deletePartition := map[int]any{1000: [16]byte(tenant), 1001: int32(7)}
	if partitionKey(dataPartition) == partitionKey(deletePartition) {
		t.Fatal("precondition: the two shapes must differ as JSON text")
	}
	bin := compactionBin{SpecID: 1, Partition: dataPartition}
	size := putIDNameDataFile(t, fs, "d.parquet", []idNameRow{{1, "x"}, {2, "y"}})
	data := entryWithSeq(t, spec, iceberg.EntryContentData, "data/d.parquet", dataPartition, 2, size, &one, nil)
	bin.Entries = []iceberg.ManifestEntry{data}
	sizeDel := putEqDeleteFile(t, fs, "eq.parquet", []idNameRow{{1, ""}})
	del := entryWithSeq(t, spec, iceberg.EntryContentEqDeletes, "data/eq.parquet", deletePartition, 1, sizeDel, &two, []int{1})

	groups, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{del}, schema, specs)
	if err != nil {
		t.Fatalf("collectEqualityDeletes: %v", err)
	}
	binGroups, err := eqDeleteGroupsForBin(groups, bin, specs, schema)
	if err != nil {
		t.Fatalf("eqDeleteGroupsForBin: %v", err)
	}
	out, _, err := mergeParquetFiles(context.Background(), client, "test-bucket", "ns/tbl", bin.Entries, nil, binGroups, schema, 0, false)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}
	if got := readIDNameRows(t, out); len(got) != 1 || got[0] != (idNameRow{2, "y"}) {
		t.Errorf("merged = %v, want only {2 y}: the delete is in the data file's partition", got)
	}

	// control: a delete of another tenant does not apply
	other := map[int]any{1000: uuid.MustParse("00000000-0000-4000-8000-000000000001"), 1001: int64(7)}
	delOther := entryWithSeq(t, spec, iceberg.EntryContentEqDeletes, "data/eq.parquet", other, 1, sizeDel, &two, []int{1})
	groups, err = collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{delOther}, schema, specs)
	if err != nil {
		t.Fatalf("collectEqualityDeletes: %v", err)
	}
	if binGroups, err = eqDeleteGroupsForBin(groups, bin, specs, schema); err != nil || len(binGroups) != 0 {
		t.Errorf("another tenant's delete: %d groups, err %v; want none", len(binGroups), err)
	}

	// a bin whose partition value does not convert is refused while a delete of its spec exists
	undecidable := compactionBin{SpecID: 1, Partition: map[int]any{1000: "not-a-uuid", 1001: int64(7)}, Entries: bin.Entries}
	if _, err := eqDeleteGroupsForBin(groups, undecidable, specs, schema); err == nil || !strings.Contains(err.Error(), "cannot be compared") {
		t.Errorf("an undecidable bin: err = %v, want the bin refused", err)
	}
	// and a delete whose value does not convert is refused when it is read
	delBad := entryWithSeq(t, spec, iceberg.EntryContentEqDeletes, "data/eq.parquet", map[int]any{1000: "not-a-uuid", 1001: int64(7)}, 1, sizeDel, &two, []int{1})
	if _, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{delBad}, schema, specs); err == nil {
		t.Error("a delete whose partition does not convert: want an error")
	}
	// a null written as nil and a null left out are one partition
	if a, err := typedPartitionKey(spec, schema, map[int]any{1000: nil, 1001: int64(7)}); err != nil {
		t.Fatal(err)
	} else if b, err := typedPartitionKey(spec, schema, map[int]any{1001: int32(7)}); err != nil || a != b {
		t.Errorf("null present %q vs missing %q (err %v): want one key", a, b, err)
	}
}

// A delete file under a partitioned spec with no partition values cannot be
// placed: read as unpartitioned by its map it would match no bin, and its
// deletes would be skipped. It is refused, and the compaction with it.
func TestADeleteWithoutPartitionValuesUnderAPartitionedSpecIsRefused(t *testing.T) {
	fs, client := startFakeFiler(t)
	schema := newTestSchema()
	byName := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1000, Name: "name", Transform: iceberg.IdentityTransform{}})
	specs := map[int]iceberg.PartitionSpec{0: *iceberg.UnpartitionedSpec, 1: byName}
	two := int64(2)
	sizeDel := putEqDeleteFile(t, fs, "eq-empty.parquet", []idNameRow{{1, ""}})
	del := entryWithSeq(t, byName, iceberg.EntryContentEqDeletes, "data/eq-empty.parquet", map[int]any{}, 1, sizeDel, &two, []int{1})

	if _, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{del}, schema, specs); err == nil ||
		!strings.Contains(err.Error(), "no partition values") {
		t.Fatalf("collectEqualityDeletes: err = %v, want the delete refused for its missing partition values", err)
	}
	// where filters keep it in, so it is refused rather than filtered away
	predicate := &partitionPredicate{Clauses: []whereClause{{Field: "name", Literals: []string{"'a'"}}}}
	if in, err := deleteInPredicateScope(del, predicate, specs, schema); err != nil || !in {
		t.Errorf("in predicate scope = %v (err %v), want true", in, err)
	}
}

// Delete groups are keyed by fields and scope with separators: spec 1 with
// partition field 11 and spec 11 with field 1 printed the same key without
// them, so one group would take both files' keys under the first one's scope
// and the other spec's bin would get no deletes.
func TestEqualityDeleteGroupsOfDifferentScopesDoNotCollide(t *testing.T) {
	fs, client := startFakeFiler(t)
	schema := newTestSchema()
	spec1 := iceberg.NewPartitionSpecID(1, iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 11, Name: "n11", Transform: iceberg.IdentityTransform{}})
	spec11 := iceberg.NewPartitionSpecID(11, iceberg.PartitionField{SourceIDs: []int{2}, FieldID: 1, Name: "n1", Transform: iceberg.IdentityTransform{}})
	specs := map[int]iceberg.PartitionSpec{0: *iceberg.UnpartitionedSpec, 1: spec1, 11: spec11}
	one, two := int64(1), int64(2)

	size11 := putEqDeleteFile(t, fs, "eq-11.parquet", []idNameRow{{1, ""}})
	del11 := entryWithSeq(t, spec11, iceberg.EntryContentEqDeletes, "data/eq-11.parquet", map[int]any{1: "x"}, 1, size11, &two, []int{1})
	size1 := putEqDeleteFile(t, fs, "eq-1.parquet", []idNameRow{{2, ""}})
	del1 := entryWithSeq(t, spec1, iceberg.EntryContentEqDeletes, "data/eq-1.parquet", map[int]any{11: "x"}, 1, size1, &two, []int{1})
	groups, err := collectEqualityDeletes(context.Background(), client, "test-bucket", "ns/tbl", []iceberg.ManifestEntry{del11, del1}, schema, specs)
	if err != nil {
		t.Fatalf("collectEqualityDeletes: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("%d groups, want 2: the two scopes are different", len(groups))
	}

	size := putIDNameDataFile(t, fs, "d1.parquet", []idNameRow{{1, "x"}, {2, "x"}})
	data := entryWithSeq(t, spec1, iceberg.EntryContentData, "data/d1.parquet", map[int]any{11: "x"}, 2, size, &one, nil)
	bin := compactionBin{SpecID: 1, Partition: map[int]any{11: "x"}, Entries: []iceberg.ManifestEntry{data}}
	binGroups, err := eqDeleteGroupsForBin(groups, bin, specs, schema)
	if err != nil {
		t.Fatalf("eqDeleteGroupsForBin: %v", err)
	}
	out, _, err := mergeParquetFiles(context.Background(), client, "test-bucket", "ns/tbl", bin.Entries, nil, binGroups, schema, 0, false)
	if err != nil {
		t.Fatalf("mergeParquetFiles: %v", err)
	}
	if got := readIDNameRows(t, out); len(got) != 1 || got[0] != (idNameRow{1, "x"}) {
		t.Errorf("spec 1's bin = %v, want only {1 x}: spec 1's delete of id 2 applies, spec 11's of id 1 does not", got)
	}
}
